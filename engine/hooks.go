package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	ErrHookValidation = errors.New("invalid hook input")
	ErrHookConflict   = errors.New("hook conflict")
	ErrHookGone       = errors.New("hook gone")
	ErrHookCapacity   = errors.New("hook storage capacity exhausted")
)

// Change the version when validation rules change; existing Hooks retain
// their original contract.
type InputContract struct {
	ID       string
	typ      reflect.Type
	validate func(json.RawMessage) error
}

// HookType accepts one JSON value of T, rejecting unknown object fields and
// null. validate checks required fields and application constraints.
func HookType[T any](version string, validate func(T) error) *InputContract {
	t := reflect.TypeFor[T]()
	return &InputContract{typ: t, ID: version + ":" + hookKey(typeShape(t, map[reflect.Type]bool{})), validate: func(data json.RawMessage) error {
		if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
			return errors.New("null input")
		}
		var input T
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&input); err != nil {
			return err
		}
		if err := dec.Decode(new(any)); err != io.EOF {
			return errors.New("expected one JSON value")
		}
		if validate != nil {
			return validate(input)
		}
		return nil
	}}
}

func typeShape(t reflect.Type, seen map[reflect.Type]bool) string {
	if seen[t] {
		return t.String()
	}
	seen[t] = true
	s := t.PkgPath() + "/" + t.String()
	switch t.Kind() {
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			s += "|" + f.Name + ":" + string(f.Tag) + ":" + typeShape(f.Type, seen)
		}
	case reflect.Pointer, reflect.Slice, reflect.Array:
		s += ":" + typeShape(t.Elem(), seen)
	case reflect.Map:
		s += ":" + typeShape(t.Key(), seen) + ":" + typeShape(t.Elem(), seen)
	}
	return s
}

type Receipt struct {
	Token        string    `json:"token"`
	SubmissionID string    `json:"submission_id"`
	AcceptedAt   time.Time `json:"accepted_at"`
}

type HookStatus string

const (
	HookPending  HookStatus = "pending"
	HookAccepted HookStatus = "accepted"
	HookConsumed HookStatus = "consumed"
	HookGone     HookStatus = "gone"
)

type Hook struct {
	Name     string          `json:"name"`
	Token    string          `json:"token"`
	RunID    string          `json:"run_id"`
	Step     int             `json:"step"`
	Contract string          `json:"contract"`
	Status   HookStatus      `json:"status"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Receipt  *Receipt        `json:"receipt,omitempty"`
	rev      uint64
}

// HookWait is registration intent and, after success, the consumption record.
type HookWait struct {
	Name     string `json:"name"`
	Token    string `json:"token"`
	Step     int    `json:"step"`
	Contract string `json:"contract"`
	Consumed string `json:"consumed,omitempty"`
}

type Submission struct {
	Token        string          `json:"token"`
	SubmissionID string          `json:"submission_id"`
	Payload      json.RawMessage `json:"payload"`
}

func hookKey(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func (e *Engine) initHooks(ctx context.Context, cfg Config) error {
	if cfg.HookPayloadLimit == 0 {
		cfg.HookPayloadLimit = 64 << 10
	}
	if cfg.HookMaxBytes == 0 {
		cfg.HookMaxBytes = 64 << 20
	}
	if cfg.HookPayloadLimit < 1 || cfg.HookMaxBytes < 1 {
		return errors.New("hook limits must be positive")
	}
	e.hookPayloadLimit = cfg.HookPayloadLimit
	var err error
	e.hooks, err = e.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: e.names.hooks, History: 1, MaxBytes: cfg.HookMaxBytes})
	return err
}

func (e *Engine) HookPayloadLimit() int { return e.hookPayloadLimit }

func (e *Engine) GetHook(ctx context.Context, token string) (*Hook, error) {
	entry, err := e.hooks.Get(ctx, hookKey(token))
	if err != nil {
		return nil, err
	}
	var h Hook
	if err = json.Unmarshal(entry.Value(), &h); err != nil {
		return nil, err
	}
	h.rev = entry.Revision()
	return &h, nil
}

func (e *Engine) saveHook(ctx context.Context, h *Hook) error {
	data, err := json.Marshal(h)
	if err != nil {
		return err
	}
	if h.rev == 0 {
		h.rev, err = e.hooks.Create(ctx, hookKey(h.Token), data)
	} else {
		var rev uint64
		rev, err = e.hooks.Update(ctx, hookKey(h.Token), data, h.rev)
		if err == nil {
			h.rev = rev
		}
	}
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode == 10077 {
		return fmt.Errorf("%w: %v", ErrHookCapacity, err)
	}
	return err
}

// RegisterHook commits intent before reserving the token. Only call from a
// Step, before contacting a callback-producing service. External calls still
// need their own idempotency keys because Step executions can overlap.
func (r *Run) RegisterHook(ctx context.Context, name, token string) (*Hook, error) {
	if r.engine == nil {
		return nil, errors.New("hook registration requires an executing Step")
	}
	e := r.engine
	contract := e.workflows[r.Workflow].Steps[r.Step].Input
	if name == "" || len(name) > 256 || len(token) > 1024 || contract == nil {
		return nil, fmt.Errorf("%w: hook name, bounded token and Step input contract required", ErrHookValidation)
	}
	for _, w := range r.HookWaits {
		if w.Name == name {
			if w.Step != r.Step || w.Contract != contract.ID || (token != "" && token != w.Token) {
				return nil, ErrHookConflict
			}
			return e.reserveHook(ctx, r, w)
		}
		if w.Consumed == "" {
			return nil, fmt.Errorf("%w: outstanding hook %s", ErrHookConflict, w.Name)
		}
	}
	if token == "" {
		token = uuid.NewString()
	}
	w := HookWait{Name: name, Token: token, Step: r.Step, Contract: contract.ID}
	stored, err := e.GetRun(ctx, r.ID)
	if err != nil {
		return nil, err
	}
	if stored.rev != r.rev || stored.Status != StatusRunning {
		return nil, ErrHookConflict
	}
	stored.HookWaits = append(stored.HookWaits, w)
	if err = e.putRun(ctx, stored); err != nil {
		return nil, err
	}
	r.HookWaits = stored.HookWaits
	r.rev = stored.rev
	return e.reserveHook(ctx, r, w)
}

func (e *Engine) reserveHook(ctx context.Context, r *Run, w HookWait) (*Hook, error) {
	h, err := e.GetHook(ctx, w.Token)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		h = &Hook{Name: w.Name, Token: w.Token, RunID: r.ID, Step: w.Step, Contract: w.Contract, Status: HookPending}
		if err = e.saveHook(ctx, h); err == nil {
			return h, nil
		}
		if !errors.Is(err, jetstream.ErrKeyExists) {
			return nil, err
		}
		h, err = e.GetHook(ctx, w.Token)
	}
	if err != nil {
		return nil, err
	}
	if h.RunID != r.ID || h.Name != w.Name || h.Step != w.Step || h.Contract != w.Contract {
		return nil, ErrHookConflict
	}
	if h.Status == HookGone {
		return nil, ErrHookGone
	}
	return h, nil
}

// Missing input returns ErrPark. Consumption commits only when the Step succeeds.
func HookInput[T any](ctx context.Context, r *Run, name, token string) (T, error) {
	var input T
	if r.engine == nil {
		return input, errors.New("HookInput requires an executing Step")
	}
	contract := r.engine.workflows[r.Workflow].Steps[r.Step].Input
	if contract == nil || contract.typ != reflect.TypeFor[T]() {
		return input, fmt.Errorf("%w: HookInput type differs from Step contract", ErrHookValidation)
	}
	h, err := r.RegisterHook(ctx, name, token)
	if err != nil {
		return input, err
	}
	if h.Receipt == nil {
		return input, ErrPark
	}
	if err = json.Unmarshal(h.Payload, &input); err != nil {
		return input, err
	}
	r.consuming = map[string]string{h.Name: h.Receipt.SubmissionID}
	return input, nil
}

// Submit's durable Hook update is the acceptance boundary. Delivery errors
// never invalidate its receipt. Callers retain both token and submission ID.
func (e *Engine) Submit(ctx context.Context, s Submission) (*Receipt, error) {
	if err := e.checkSubmission(s); err != nil {
		return nil, err
	}
	normalized, err := normalizeJSON(s.Payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrHookValidation, err)
	}
	for {
		h, err := e.GetHook(ctx, s.Token)
		if err != nil {
			return nil, err
		}
		if h.Status == HookGone {
			return nil, ErrHookGone
		}
		if h.Receipt != nil {
			if h.Receipt.SubmissionID == s.SubmissionID && equivalentJSON(h.Payload, normalized) {
				return h.Receipt, nil
			}
			return nil, ErrHookConflict
		}
		r, err := e.GetRun(ctx, h.RunID)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				return nil, ErrHookGone
			}
			return nil, err
		}
		if r.Status != StatusRunning && r.Status != StatusParked {
			return nil, ErrHookConflict
		}
		var matching bool
		for _, w := range r.HookWaits {
			if w.Token == h.Token && w.Name == h.Name && w.Step == r.Step && w.Contract == h.Contract && w.Consumed == "" {
				matching = true
			}
		}
		wf, ok := e.workflows[r.Workflow]
		if !matching || !ok || h.Step < 0 || h.Step >= len(wf.Steps) {
			return nil, ErrHookConflict
		}
		contract := wf.Steps[h.Step].Input
		if contract == nil || contract.ID != h.Contract {
			return nil, ErrHookConflict
		}
		if err = contract.validate(s.Payload); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrHookValidation, err)
		}
		h.Payload = normalized
		h.Receipt = &Receipt{Token: h.Token, SubmissionID: s.SubmissionID, AcceptedAt: time.Now().UTC()}
		h.Status = HookAccepted
		if err = e.saveHook(ctx, h); errors.Is(err, jetstream.ErrKeyExists) {
			continue
		} else if err != nil {
			return nil, err
		}
		e.reconcileRunHooks(ctx, r)
		return h.Receipt, nil
	}
}

func normalizeJSON(data []byte) (json.RawMessage, error) {
	var value any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return nil, errors.New("expected one JSON value")
	}
	return json.Marshal(value)
}

func equivalentJSON(a, b []byte) bool {
	decode := func(data []byte) any {
		var v any
		d := json.NewDecoder(bytes.NewReader(data))
		d.UseNumber()
		_ = d.Decode(&v)
		return v
	}
	var eq func(any, any) bool
	eq = func(a, b any) bool {
		switch x := a.(type) {
		case json.Number:
			y, ok := b.(json.Number)
			if !ok {
				return false
			}
			return canonicalDecimal(string(x)) == canonicalDecimal(string(y))
		case map[string]any:
			y, ok := b.(map[string]any)
			if !ok || len(x) != len(y) {
				return false
			}
			for k, v := range x {
				w, ok := y[k]
				if !ok || !eq(v, w) {
					return false
				}
			}
			return true
		case []any:
			y, ok := b.([]any)
			if !ok || len(x) != len(y) {
				return false
			}
			for i, v := range x {
				if !eq(v, y[i]) {
					return false
				}
			}
			return true
		default:
			return reflect.DeepEqual(a, b)
		}
	}
	return eq(decode(a), decode(b))
}

// Resume is a Run-scoped adapter. It never resolves an old request to a later
// wait. Identical retries can recover a receipt even after consumption.
func (e *Engine) Resume(ctx context.Context, runID string, s Submission) (*Receipt, error) {
	if err := e.checkSubmission(s); err != nil {
		return nil, err
	}
	h, err := e.GetHook(ctx, s.Token)
	if err != nil {
		return nil, err
	}
	if h.RunID != runID {
		return nil, ErrHookConflict
	}
	return e.Submit(ctx, s)
}

func (e *Engine) RunHooks(ctx context.Context, r *Run) ([]*Hook, error) {
	hooks := make([]*Hook, 0, len(r.HookWaits))
	for _, w := range r.HookWaits {
		h, err := e.GetHook(ctx, w.Token)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if h.RunID != r.ID || h.Name != w.Name {
			continue
		}
		hooks = append(hooks, h)
	}
	return hooks, nil
}

func (e *Engine) reconcileRunHooks(ctx context.Context, r *Run) {
	for _, w := range r.HookWaits {
		h, err := e.reserveHook(ctx, r, w)
		if err != nil {
			continue
		}
		if w.Consumed != "" && h.Receipt != nil && h.Receipt.SubmissionID == w.Consumed {
			if h.Status != HookConsumed {
				h.Status = HookConsumed
				_ = e.saveHook(ctx, h)
			}
		} else if h.Receipt != nil && r.Status == StatusParked && w.Step == r.Step {
			r.Status = StatusRunning
			r.Execution = uuid.NewString()
			if e.putRun(ctx, r) != nil {
				return
			}
		}
	}
	if r.Status == StatusRunning {
		_ = e.enqueue(ctx, r, r.Step)
	}
}

func (e *Engine) cleanHooks(ctx context.Context) {
	keys, err := e.hooks.ListKeys(ctx)
	if err != nil {
		return
	}
	for key := range keys.Keys() {
		entry, err := e.hooks.Get(ctx, key)
		if err != nil {
			continue
		}
		var h Hook
		if json.Unmarshal(entry.Value(), &h) != nil || h.Status == HookGone {
			continue
		}
		h.rev = entry.Revision()
		if _, err = e.GetRun(ctx, h.RunID); !errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		h.Status = HookGone
		h.Payload = nil
		h.Receipt = nil
		_ = e.saveHook(ctx, &h)
	}
}

func canonicalDecimal(s string) string {
	sign := ""
	if strings.HasPrefix(s, "-") {
		sign = "-"
		s = s[1:]
	}
	exponent := new(big.Int)
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		exponent.SetString(s[i+1:], 10)
		s = s[:i]
	}
	if i := strings.IndexByte(s, '.'); i >= 0 {
		exponent.Sub(exponent, big.NewInt(int64(len(s)-i-1)))
		s = s[:i] + s[i+1:]
	}
	s = strings.TrimLeft(s, "0")
	if s == "" {
		return "0"
	}
	trimmed := strings.TrimRight(s, "0")
	exponent.Add(exponent, big.NewInt(int64(len(s)-len(trimmed))))
	return sign + trimmed + "e" + exponent.String()
}

func (e *Engine) checkSubmission(s Submission) error {
	if s.Token == "" || len(s.Token) > 1024 || s.SubmissionID == "" || len(s.SubmissionID) > 256 || len(s.Payload) == 0 || len(s.Payload) > e.hookPayloadLimit {
		return ErrHookValidation
	}
	return nil
}

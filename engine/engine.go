package engine

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Config struct {
	// Empty preserves legacy resource names. Workers sharing a namespace must
	// use compatible workflows and configuration. Allowed: 1-64 ASCII letters,
	// digits, underscores or hyphens.
	Namespace string
	// The first delay overrides AckWait and must allow the slowest Step to
	// finish. Empty defaults to 2m, 2m, 3m, 4m.
	BackOff []time.Duration
	// Must be at least len(BackOff)+1. Zero defaults to 5.
	MaxDeliver int
	// Zero defaults to 64 KiB.
	HookPayloadLimit int
	// Includes permanent token reservations. Zero defaults to 64 MiB.
	HookMaxBytes int64
	// Rejects new work when full. Zero defaults to 64 MiB.
	StepMaxBytes int64
}

// Wrap with %w to fail immediately without using the remaining retry budget.
var ErrTerminal = errors.New("terminal")

// State changes survive ErrPark; other Step errors discard them.
var ErrPark = errors.New("waiting for input")

type Status string

const (
	StatusScheduled Status = "scheduled"
	StatusCancelled Status = "cancelled"
	StatusRunning   Status = "running"
	StatusParked    Status = "parked"
	StatusDone      Status = "done"
	StatusFailed    Status = "failed"
)

func (s Status) Terminal() bool { return s == StatusDone || s == StatusFailed || s == StatusCancelled }

func (s Status) Parked() bool { return s == StatusParked }

type Step struct {
	Name  string
	Input *InputContract
	Run   func(ctx context.Context, run *Run) error
}

type Workflow struct {
	Name  string
	Steps []Step
	// Optional; nil produces an empty result.
	Result func(*Run) string
}

type Run struct {
	ID        string     `json:"id"`
	Workflow  string     `json:"workflow"`
	Status    Status     `json:"status"`
	Step      int        `json:"step"`
	Error     string     `json:"error,omitempty"`
	StartAt   *time.Time `json:"start_at,omitempty"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	// StartGeneration identifies the scheduling revision that claimed execution.
	StartGeneration uint64          `json:"start_generation,omitempty"`
	State           json.RawMessage `json:"state,omitempty"`

	// The KV timestamp, populated on read rather than stored in the Run JSON.
	Updated time.Time `json:"-"`
	// Used for compare-and-swap, independently of execution identity.
	rev       uint64
	Execution string     `json:"execution,omitempty"`
	Hooks     []*Hook    `json:"-"`
	HookWaits []HookWait `json:"hook_waits,omitempty"`
	engine    *Engine
	consuming map[string]string
}

func Typed[S any](fn func(ctx context.Context, run *Run, s *S) error) func(ctx context.Context, run *Run) error {
	return func(ctx context.Context, run *Run) error {
		var s S
		if len(run.State) > 0 {
			if err := json.Unmarshal(run.State, &s); err != nil {
				return fmt.Errorf("%w: decoding %T state: %v", ErrTerminal, s, err)
			}
		}
		if err := fn(ctx, run, &s); err != nil {
			if errors.Is(err, ErrPark) {
				if data, merr := json.Marshal(s); merr == nil {
					run.State = data
				}
			}
			return err
		}
		data, err := json.Marshal(s)
		if err != nil {
			return err
		}
		run.State = data
		return nil
	}
}

// A Run with no State yields the zero value of S.
func StateOf[S any](run *Run) (S, error) {
	var s S
	if len(run.State) == 0 {
		return s, nil
	}
	err := json.Unmarshal(run.State, &s)
	return s, err
}

type StepMsg struct {
	RunID     string `json:"run_id"`
	Step      int    `json:"step"`
	Execution string `json:"execution,omitempty"`
}

// An Engine must not be copied after first use.
type Engine struct {
	names            resourceNames
	serving          atomic.Bool
	hooks            jetstream.KeyValue
	hookPayloadLimit int
	js               jetstream.JetStream
	kv               jetstream.KeyValue
	cons             jetstream.Consumer
	schedules        jetstream.Stream
	starts           jetstream.Consumer
	workflows        map[string]Workflow
	maxDeliver       int
	log              *slog.Logger
}

// New provisions storage but does not start workers or own nc.
// A nil logger uses slog.Default.
func New(ctx context.Context, nc *nats.Conn, log *slog.Logger, cfg Config, wfs ...Workflow) (*Engine, error) {
	names, err := namesFor(cfg.Namespace)
	if err != nil {
		return nil, err
	}
	workflows, err := registeredWorkflows(wfs)
	if err != nil {
		return nil, err
	}
	if nc == nil || nc.IsClosed() {
		return nil, errors.New("an open NATS connection is required")
	}
	if log == nil {
		log = slog.Default()
	}
	if len(cfg.BackOff) == 0 {
		cfg.BackOff = []time.Duration{2 * time.Minute, 2 * time.Minute, 3 * time.Minute, 4 * time.Minute}
	}
	if cfg.MaxDeliver == 0 {
		cfg.MaxDeliver = 5
	}
	if cfg.MaxDeliver < len(cfg.BackOff)+1 {
		return nil, errors.New("MaxDeliver must be at least len(BackOff)+1")
	}
	for _, delay := range cfg.BackOff {
		if delay <= 0 {
			return nil, errors.New("BackOff durations must be positive")
		}
	}
	if cfg.HookPayloadLimit < 0 || cfg.HookMaxBytes < 0 {
		return nil, errors.New("hook limits must not be negative")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	if cfg.StepMaxBytes == 0 {
		cfg.StepMaxBytes = 64 << 20
	}
	if cfg.StepMaxBytes < 1 {
		return nil, errors.New("step capacity must be positive")
	}
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       names.steps,
		MaxBytes:   cfg.StepMaxBytes,
		Discard:    jetstream.DiscardNew,
		Subjects:   []string{names.subject("step.>")},
		Retention:  jetstream.WorkQueuePolicy,
		Storage:    jetstream.FileStorage,
		Duplicates: 2 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("creating %s stream: %w", names.steps, err)
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:    "executor",
		AckPolicy:  jetstream.AckExplicitPolicy,
		BackOff:    cfg.BackOff,
		AckWait:    cfg.BackOff[0],
		MaxDeliver: cfg.MaxDeliver,
	})
	if err != nil {
		return nil, fmt.Errorf("creating executor consumer: %w", err)
	}

	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  names.runs,
		History: 64,
	})
	if err != nil {
		return nil, fmt.Errorf("creating %s bucket: %w", names.runs, err)
	}

	e := &Engine{names: names, js: js, kv: kv, cons: cons, workflows: workflows, maxDeliver: cfg.MaxDeliver, log: log}
	if err := e.initHooks(ctx, cfg); err != nil {
		return nil, err
	}
	if err := e.initSchedules(ctx); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *Engine) Start(ctx context.Context, wf string, initial any) (string, error) {
	return e.createRun(ctx, wf, initial, nil)
}

func (e *Engine) createRun(ctx context.Context, wf string, initial any, at *time.Time) (string, error) {
	if _, ok := e.workflows[wf]; !ok {
		return "", fmt.Errorf("unknown workflow %q", wf)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	run := &Run{ID: id.String(), Workflow: wf, Status: StatusRunning, Execution: uuid.NewString()}
	if at != nil {
		run.Status = StatusScheduled
		run.StartAt = at
	}
	if initial != nil {
		data, err := json.Marshal(initial)
		if err != nil {
			return "", err
		}
		run.State = data
	}
	if err := e.putRun(ctx, run); err != nil {
		return "", err
	}
	if at != nil {
		// The durable intent is accepted. Serve repairs a failed publication.
		if err := e.publishSchedule(ctx, run); err != nil {
			e.log.Warn("schedule publication deferred", "run", run.ID, "err", err)
		}
		return run.ID, nil
	}
	return run.ID, e.enqueue(ctx, run, 0)
}

// Retry preserves earlier Steps and gives the failed Step a fresh retry budget.
func (e *Engine) Retry(ctx context.Context, runID string) error {
	run, err := e.GetRun(ctx, runID)
	if err != nil {
		return err
	}
	if run.Status.Parked() {
		return fmt.Errorf("run %s is parked, not failed — resume it instead of retrying", runID)
	}
	if run.Status != StatusFailed {
		return fmt.Errorf("run %s is not failed (status %s)", runID, run.Status)
	}
	run.Status = StatusRunning
	run.Execution = uuid.NewString()
	run.Error = ""
	if err := e.putRun(ctx, run); err != nil {
		return err
	}
	return e.enqueue(ctx, run, run.Step)
}

var ErrServing = errors.New("engine is already serving")

// Cancellation returns nil after active callbacks finish; Steps must honor
// context cancellation. Serve never closes the caller's NATS connection.
// Concurrent Serve calls on the same Engine return ErrServing.
func (e *Engine) Serve(ctx context.Context) error {
	if !e.serving.CompareAndSwap(false, true) {
		return ErrServing
	}
	defer e.serving.Store(false)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if ctx.Err() != nil {
		return nil
	}
	var consumers []jetstream.ConsumeContext
	var callbacks sync.RWMutex
	defer func() {
		cancel()
		for _, consumer := range consumers {
			consumer.Stop()
		}
		// A subscription can become invalid before its callback returns.
		// Wait for our handlers directly; later callbacks see cancelled ctx
		// and cannot enter engine or application code.
		callbacks.Lock()
		callbacks.Unlock()
		for _, consumer := range consumers {
			<-consumer.Closed()
		}
	}()
	sc, err := e.starts.Consume(func(msg jetstream.Msg) {
		callbacks.RLock()
		defer callbacks.RUnlock()
		if ctx.Err() == nil {
			e.handleStart(ctx, msg)
		}
	})
	if err != nil {
		return err
	}
	consumers = append(consumers, sc)
	cc, err := e.cons.Consume(func(msg jetstream.Msg) {
		callbacks.RLock()
		defer callbacks.RUnlock()
		if ctx.Err() == nil {
			e.handle(ctx, msg)
		}
	})
	if err != nil {
		return err
	}
	consumers = append(consumers, cc)
	startsClosed, stepsClosed := sc.Closed(), cc.Closed()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		e.maintainRuns(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-startsClosed:
			return errors.New("schedule consumer stopped unexpectedly")
		case <-stepsClosed:
			return errors.New("step consumer stopped unexpectedly")
		case <-ticker.C:
		}
	}
}

// Persist State, publish the next Step, then ack. A crash between these
// operations leaves a delivery that can repair the interrupted transition.
// See docs/adr/0001.
func (e *Engine) handle(ctx context.Context, msg jetstream.Msg) {
	var sm StepMsg
	if err := json.Unmarshal(msg.Data(), &sm); err != nil {
		e.log.Error("malformed step message", "err", err)
		msg.Term()
		return
	}
	run, err := e.GetRun(ctx, sm.RunID)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			msg.Ack()
			return
		}
		e.log.Error("loading run", "run", sm.RunID, "err", err)
		msg.Nak()
		return
	}
	wf := e.workflows[run.Workflow]
	if run.Status == StatusScheduled || run.Status == StatusParked {
		msg.Ack()
		return
	}

	// Redelivery of a step that already completed. If the run is still going,
	// re-enqueue its current step in case the crash happened between putRun and
	// enqueue — dedup makes this a no-op when the message already exists.
	if run.Step > sm.Step || run.Status.Terminal() {
		if run.Status == StatusRunning {
			e.enqueue(ctx, run, run.Step)
		}
		msg.Ack()
		return
	}

	if sm.Step != run.Step || sm.Execution != run.Execution || sm.Step < 0 || sm.Step >= len(wf.Steps) {
		msg.Ack()
		return
	}
	run.engine = e
	step := wf.Steps[sm.Step]
	e.log.Info("step start", "run", run.ID, "step", sm.Step, "name", step.Name)
	if err := step.Run(ctx, run); err != nil {
		if errors.Is(err, ErrPark) {
			run.Status = StatusParked
			if perr := e.putRun(ctx, run); perr != nil {
				msg.Nak()
				return
			}
			msg.Ack()
			e.log.Info("step parked", "run", run.ID, "step", sm.Step, "name", step.Name, "why", err)
			return
		}
		e.fail(ctx, msg, run, err)
		return
	}
	for i := range run.HookWaits {
		w := &run.HookWaits[i]
		if id := run.consuming[w.Name]; id != "" {
			w.Consumed = id
		}
	}
	run.Execution = uuid.NewString()
	run.Step = sm.Step + 1

	if run.Step == len(wf.Steps) {
		run.Status = StatusDone
	}
	if err := e.putRun(ctx, run); err != nil {
		msg.Nak()
		return
	}
	if run.Status != StatusDone {
		if err := e.enqueue(ctx, run, run.Step); err != nil {
			msg.Nak()
			return
		}
	}
	msg.Ack()
	e.log.Info("step done", "run", run.ID, "step", sm.Step, "name", step.Name, "status", run.Status)
}

func (e *Engine) fail(ctx context.Context, msg jetstream.Msg, run *Run, err error) {
	// NumDelivered is native server metadata; on the last attempt we record the
	// failure ourselves rather than relying on the MAX_DELIVERIES advisory,
	// which is fire-and-forget and would leave the run stuck if missed.
	md, mdErr := msg.Metadata()
	if !errors.Is(err, ErrTerminal) && mdErr == nil && md.NumDelivered < uint64(e.maxDeliver) {
		e.log.Warn("step failed, will retry", "run", run.ID, "step", run.Step, "attempt", md.NumDelivered, "err", err)
		msg.Nak()
		return
	}
	run.Status = StatusFailed
	run.Error = err.Error()
	if err := e.putRun(ctx, run); err != nil {
		msg.Nak()
		return
	}
	msg.Term()
	e.log.Error("run failed", "run", run.ID, "step", run.Step, "err", err)
}

// enqueue republishes the persisted execution identity. Dedup reduces copies;
// the Step and execution checks reject stale deliveries beyond its window.
func (e *Engine) enqueue(ctx context.Context, run *Run, step int) error {
	data, err := json.Marshal(StepMsg{RunID: run.ID, Step: step, Execution: run.Execution})
	if err != nil {
		return err
	}
	_, err = e.js.Publish(ctx, e.names.subject("step."+run.ID), data,
		jetstream.WithMsgID(fmt.Sprintf("%s:%d:%s", run.ID, step, run.Execution)))
	return err
}

func (e *Engine) GetRun(ctx context.Context, id string) (*Run, error) {
	entry, err := e.kv.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("run %s: %w", id, err)
	}
	run, err := runFromEntry(entry)
	if err != nil {
		return nil, err
	}
	run.Hooks, err = e.RunHooks(ctx, run)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		err = nil
	}
	return run, err
}

func runFromEntry(entry jetstream.KeyValueEntry) (*Run, error) {
	var run Run
	if err := json.Unmarshal(entry.Value(), &run); err != nil {
		return nil, err
	}
	run.Updated = entry.Created().UTC()
	run.rev = entry.Revision()
	return &run, nil
}

// History is limited to the latest 64 stored revisions.
func (e *Engine) RunHistory(ctx context.Context, id string) ([]*Run, error) {
	entries, err := e.kv.History(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("run %s: %w", id, err)
	}
	var runs []*Run
	for _, entry := range entries {
		if entry.Operation() != jetstream.KeyValuePut {
			continue
		}
		run, err := runFromEntry(entry)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, nil
}

func (e *Engine) ListRuns(ctx context.Context) ([]*Run, error) {
	lister, err := e.kv.ListKeys(ctx)
	if err != nil {
		return nil, err
	}
	var runs []*Run
	for key := range lister.Keys() {
		run, err := e.GetRun(ctx, key)
		if err != nil {
			e.log.Warn("skipping unreadable run", "key", key, "err", err)
			continue
		}
		runs = append(runs, run)
	}
	slices.SortFunc(runs, func(a, b *Run) int { return b.Updated.Compare(a.Updated) })
	return runs, nil
}

func (e *Engine) Workflows() []Workflow {
	wfs := slices.Collect(maps.Values(e.workflows))
	for i := range wfs {
		wfs[i] = cloneWorkflow(wfs[i])
	}
	slices.SortFunc(wfs, func(a, b Workflow) int { return cmp.Compare(a.Name, b.Name) })
	return wfs
}

func (e *Engine) Result(run *Run) string {
	wf, ok := e.workflows[run.Workflow]
	if !ok || wf.Result == nil {
		return ""
	}
	return wf.Result(run)
}

// Returns "-" when the workflow is unknown or the Run has passed its last Step.
func (e *Engine) StepName(run *Run) string {
	wf, ok := e.workflows[run.Workflow]
	if !ok || run.Step >= len(wf.Steps) {
		return "-"
	}
	return wf.Steps[run.Step].Name
}

func (e *Engine) putRun(ctx context.Context, run *Run) error {
	if run.Status.Terminal() {
		if run.ExpiresAt == nil {
			expiry := time.Now().UTC().Add(7 * 24 * time.Hour)
			run.ExpiresAt = &expiry
		}
	} else {
		run.ExpiresAt = nil
	}
	data, err := json.Marshal(run)
	if err != nil {
		return err
	}
	var rev uint64
	if run.rev == 0 {
		rev, err = e.kv.Create(ctx, run.ID, data)
	} else {
		rev, err = e.kv.Update(ctx, run.ID, data, run.rev)
	}
	if err != nil {
		return err
	}
	run.rev = rev
	return nil
}

// With no statuses, Wait uses terminal statuses. The watcher sends the
// current value first, so an already-terminal Run returns immediately.
func (e *Engine) Wait(ctx context.Context, runID string, statuses ...Status) (*Run, error) {
	reached := func(s Status) bool { return s.Terminal() }
	if len(statuses) > 0 {
		reached = func(s Status) bool { return slices.Contains(statuses, s) }
	}
	w, err := e.kv.Watch(ctx, runID)
	if err != nil {
		return nil, err
	}
	defer w.Stop()
	var last *Run
	for {
		select {
		case <-ctx.Done():
			return last, errors.New("timed out waiting for run")
		case entry := <-w.Updates():
			if entry == nil { // end of initial values
				if last == nil {
					return nil, fmt.Errorf("run %s: not found", runID)
				}
				continue
			}
			if entry.Operation() != jetstream.KeyValuePut {
				continue
			}
			run, err := runFromEntry(entry)
			if err != nil {
				return nil, err
			}
			if reached(run.Status) {
				run.Hooks, err = e.RunHooks(ctx, run)
				return run, err
			}
			last = run
		}
	}
}

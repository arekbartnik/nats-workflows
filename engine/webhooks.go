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
	"net/http"
	"net/url"
	"path"
	"reflect"

	"github.com/nats-io/nats.go/jetstream"
)

// Request is what a Webhook's verify function sees of an incoming call.
type Request struct {
	Header http.Header
	Body   []byte
}

// WebhookType is a HookType for calls from services the application does not
// control. Unknown object fields are allowed, because such services add fields
// without notice. verify authenticates each call before anything is stored.
// accept, when not nil, declines input the Step does not wait for; a declined
// call is answered as delivered and the Hook keeps waiting.
func WebhookType[T any](version string, verify func(Request) error, accept func(T) bool) *InputContract {
	if verify == nil {
		panic("engine: WebhookType requires a verify function")
	}
	decode := func(data json.RawMessage) (T, error) {
		var input T
		if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
			return input, errors.New("null input")
		}
		dec := json.NewDecoder(bytes.NewReader(data))
		if err := dec.Decode(&input); err != nil {
			return input, err
		}
		if err := dec.Decode(new(any)); err != io.EOF {
			return input, errors.New("expected one JSON value")
		}
		return input, nil
	}
	t := reflect.TypeFor[T]()
	c := &InputContract{
		typ:    t,
		ID:     "webhook:" + version + ":" + hookKey(typeShape(t, map[reflect.Type]bool{})),
		verify: verify,
		validate: func(data json.RawMessage) error {
			_, err := decode(data)
			return err
		},
	}
	if accept != nil {
		c.accept = func(data json.RawMessage) bool {
			input, err := decode(data)
			return err == nil && accept(input)
		}
	}
	return c
}

// Webhook registers the named Hook with an engine-generated token and returns
// the address that delivers to it. Call it before handing the address to the
// calling service; HookInput with the same name then waits for the call.
func (r *Run) Webhook(ctx context.Context, name string) (string, error) {
	if r.engine == nil {
		return "", errors.New("Webhook requires an executing Step")
	}
	if r.engine.webhookURL == "" {
		return "", errors.New("Webhook requires Config.WebhookURL")
	}
	contract := r.engine.workflows[r.Workflow].Steps[r.Step].Input
	if contract == nil || contract.verify == nil {
		return "", fmt.Errorf("%w: Webhook requires a Step input contract created by WebhookType", ErrHookValidation)
	}
	h, err := r.RegisterHook(ctx, name, "")
	if err != nil {
		return "", err
	}
	return r.engine.webhookURL + "/" + url.PathEscape(h.Token), nil
}

// Webhooks serves POST <Config.WebhookURL>/<token>; mount it there. The body
// is the Hook input and its SHA-256 the submission ID, so a repeated call is
// a duplicate. Calls that cannot change anything any more are answered 202, so
// callers stop retrying them.
func (e *Engine) Webhooks() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, int64(e.hookPayloadLimit)))
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
			return
		} else if err != nil {
			http.Error(w, "unreadable body", http.StatusBadRequest)
			return
		}
		token := path.Base(r.URL.Path)
		contract, err := e.webhookContract(r.Context(), token)
		switch {
		case errors.Is(err, ErrHookGone):
			w.WriteHeader(http.StatusAccepted)
			return
		case errors.Is(err, jetstream.ErrKeyNotFound):
			http.NotFound(w, r)
			return
		case err != nil:
			e.log.Error("webhook lookup", "error", err)
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		if err = contract.verify(Request{Header: r.Header, Body: body}); err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		sum := sha256.Sum256(body)
		_, err = e.Submit(r.Context(), Submission{Token: token, SubmissionID: hex.EncodeToString(sum[:]), Payload: body})
		switch {
		case err == nil, errors.Is(err, ErrHookRefused), errors.Is(err, ErrHookConflict), errors.Is(err, ErrHookGone):
			w.WriteHeader(http.StatusAccepted)
		case errors.Is(err, ErrHookValidation):
			http.Error(w, err.Error(), http.StatusBadRequest)
		case errors.Is(err, jetstream.ErrKeyNotFound):
			http.NotFound(w, r)
		default:
			e.log.Error("webhook submit", "error", err)
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		}
	})
}

// webhookContract returns jetstream.ErrKeyNotFound for tokens that are not
// Webhooks, so plain Hooks stay unreachable from outside.
func (e *Engine) webhookContract(ctx context.Context, token string) (*InputContract, error) {
	h, err := e.GetHook(ctx, token)
	if err != nil {
		return nil, err
	}
	if h.Status == HookGone {
		return nil, ErrHookGone
	}
	run, err := e.GetRun(ctx, h.RunID)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, ErrHookGone
	} else if err != nil {
		return nil, err
	}
	wf, ok := e.workflows[run.Workflow]
	if !ok || h.Step < 0 || h.Step >= len(wf.Steps) {
		return nil, jetstream.ErrKeyNotFound
	}
	contract := wf.Steps[h.Step].Input
	if contract == nil || contract.ID != h.Contract || contract.verify == nil {
		return nil, jetstream.ErrKeyNotFound
	}
	return contract, nil
}

package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arekbartnik/nats-workflows/engine"
	"github.com/arekbartnik/nats-workflows/examples/workflows"
	"github.com/nats-io/nats.go/jetstream"
)

type hookInput struct {
	Answer string  `json:"answer"`
	Count  float64 `json:"count"`
}

var hookContract = engine.HookType[hookInput]("answer-v1", func(v hookInput) error {
	if v.Answer == "" {
		return errors.New("answer required")
	}
	return nil
})

func hookStep(name, token string) engine.Step {
	return engine.Step{Name: name, Input: hookContract, Run: func(ctx context.Context, r *engine.Run) error {
		input, err := engine.HookInput[hookInput](ctx, r, name, token)
		if err != nil {
			return err
		}
		r.State, _ = json.Marshal(input)
		return nil
	}}
}
func submission(token, id string) engine.Submission {
	return engine.Submission{Token: token, SubmissionID: id, Payload: json.RawMessage(`{"answer":"yes","count":1}`)}
}

func TestHookWinnerAndLaterWait(t *testing.T) {
	e, js := newTestEngine(t, engine.Workflow{Name: "hooks", Steps: []engine.Step{hookStep("first", "a.> / token"), hookStep("second", "")}})
	ctx := context.Background()
	id, err := e.Start(ctx, "hooks", nil)
	if err != nil {
		t.Fatal(err)
	}
	r := wait(t, e, id, engine.StatusParked)
	token := r.HookWaits[0].Token
	bad := submission(token, "bad")
	bad.Payload = []byte(`{"answer":"yes","unrelated_state":true}`)
	if _, err = e.Submit(ctx, bad); !errors.Is(err, engine.ErrHookValidation) {
		t.Fatalf("unknown field: %v", err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var receipts []*engine.Receipt
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			receipt, err := e.Submit(ctx, submission(token, fmt.Sprint(i)))
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				receipts = append(receipts, receipt)
			} else if !errors.Is(err, engine.ErrHookConflict) {
				t.Errorf("submit: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if len(receipts) != 1 {
		t.Fatalf("winners %d", len(receipts))
	}
	winner := receipts[0]
	dup := submission(token, winner.SubmissionID)
	dup.Payload = []byte(` { "count": 1.0, "answer": "yes" } `)
	receipt, err := e.Submit(ctx, dup)
	if err != nil || *receipt != *winner {
		t.Fatalf("duplicate: %+v %v", receipt, err)
	}
	dup.Payload = []byte(`{"answer":"no","count":1}`)
	if _, err = e.Submit(ctx, dup); !errors.Is(err, engine.ErrHookConflict) {
		t.Fatal(err)
	}
	r = wait(t, e, id, engine.StatusParked)
	if r.Step != 1 || r.HookWaits[0].Consumed != winner.SubmissionID {
		t.Fatalf("next wait: %+v", r)
	}
	stale, _ := json.Marshal(engine.StepMsg{RunID: id, Step: 1, Execution: "old-wakeup"})
	js.Publish(ctx, "wf.step."+id, stale)
	if _, err = e.Resume(ctx, id, submission(token, "late")); !errors.Is(err, engine.ErrHookConflict) {
		t.Fatal(err)
	}
	if _, err = e.Submit(ctx, submission(r.HookWaits[1].Token, "second")); err != nil {
		t.Fatal(err)
	}
	if r = wait(t, e, id); r.Status != engine.StatusDone {
		t.Fatal(r.Status)
	}
	if _, err = e.Submit(ctx, submission("unknown", "x")); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatal(err)
	}
}

func TestHookCallbackBetweenCheckAndPark(t *testing.T) {
	checked := make(chan string, 1)
	release := make(chan struct{})
	step := engine.Step{Name: "early", Input: hookContract, Run: engine.Typed(func(ctx context.Context, r *engine.Run, s *hookInput) error {
		input, err := engine.HookInput[hookInput](ctx, r, "early", "")
		if errors.Is(err, engine.ErrPark) {
			checked <- r.HookWaits[0].Token
			<-release
			s.Answer = "state from running step"
			return err
		}
		if err == nil {
			*s = input
		}
		return err
	})}
	e, _ := newTestEngine(t, engine.Workflow{Name: "early", Steps: []engine.Step{step}})
	ctx := context.Background()
	id, err := e.Start(ctx, "early", nil)
	if err != nil {
		t.Fatal(err)
	}
	var token string
	select {
	case token = <-checked:
	case <-time.After(5 * time.Second):
		t.Fatal("no registration")
	}
	if _, err = e.Submit(ctx, submission(token, "early")); err != nil {
		t.Fatal(err)
	}
	r, err := e.GetRun(ctx, id)
	if err != nil || r.Status != engine.StatusRunning || len(r.State) > 0 {
		t.Fatalf("overwrote executing State: %+v %v", r, err)
	}
	close(release)
	if r = wait(t, e, id); r.Status != engine.StatusDone {
		t.Fatal(r.Status)
	}
}

func TestHookFailureRetryKeepsInput(t *testing.T) {
	step := hookStep("answer", "")
	original := step.Run
	fail := true
	step.Run = func(ctx context.Context, r *engine.Run) error {
		err := original(ctx, r)
		if err != nil {
			return err
		}
		if fail {
			return engine.ErrTerminal
		}
		return nil
	}
	e, _ := newTestEngine(t, engine.Workflow{Name: "retry-hook", Steps: []engine.Step{step}})
	ctx := context.Background()
	id, _ := e.Start(ctx, "retry-hook", nil)
	r := wait(t, e, id, engine.StatusParked)
	s := submission(r.HookWaits[0].Token, "one")
	receipt, err := e.Submit(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	r = wait(t, e, id)
	if r.Status != engine.StatusFailed {
		t.Fatal(r.Status)
	}
	if _, err = e.Submit(ctx, submission(s.Token, "two")); !errors.Is(err, engine.ErrHookConflict) {
		t.Fatal(err)
	}
	again, err := e.Submit(ctx, s)
	if err != nil || *again != *receipt {
		t.Fatalf("receipt on failure: %v", err)
	}
	fail = false
	if err = e.Retry(ctx, id); err != nil {
		t.Fatal(err)
	}
	r = wait(t, e, id)
	if r.Status != engine.StatusDone || r.HookWaits[0].Consumed != "one" {
		t.Fatalf("retry: %+v", r)
	}
}

func TestHookRegistrationRecoveryAndTokenConflict(t *testing.T) {
	nc, js := startEmbedded(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	wf := engine.Workflow{Name: "recover-hook", Steps: []engine.Step{hookStep("answer", "shared")}}
	e, err := engine.New(ctx, nc, slog.New(slog.NewTextHandler(io.Discard, nil)), engine.Config{BackOff: []time.Duration{30 * time.Millisecond}, MaxDeliver: 2}, wf)
	if err != nil {
		t.Fatal(err)
	}
	kv, _ := js.KeyValue(ctx, "wf_runs")
	r := engine.Run{ID: "interrupted", Workflow: wf.Name, Status: engine.StatusRunning, Execution: "initial", HookWaits: []engine.HookWait{{Name: "answer", Token: "shared", Step: 0, Contract: hookContract.ID}}}
	data, _ := json.Marshal(r)
	kv.Create(ctx, r.ID, data)
	go e.Serve(ctx)
	parked := wait(t, e, r.ID, engine.StatusParked)
	if len(parked.Hooks) != 1 {
		t.Fatal("registration not recovered")
	}
	id, err := e.Start(ctx, wf.Name, nil)
	if err != nil {
		t.Fatal(err)
	}
	other := wait(t, e, id)
	if other.Status != engine.StatusFailed {
		t.Fatal(other.Status)
	}
	if _, err = e.Resume(ctx, id, submission("shared", "wrong-owner")); !errors.Is(err, engine.ErrHookConflict) {
		t.Fatal(err)
	}
	if _, err = e.Submit(ctx, submission("shared", "right-owner")); err != nil {
		t.Fatal(err)
	}
	if got := wait(t, e, r.ID); got.Status != engine.StatusDone {
		t.Fatal(got.Status)
	}
}

func TestHookAcceptedOfflineAndCleanup(t *testing.T) {
	nc, js := startEmbedded(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wf := engine.Workflow{Name: "offline-hook", Steps: []engine.Step{hookStep("answer", "")}}
	cfg := engine.Config{BackOff: []time.Duration{30 * time.Millisecond}, MaxDeliver: 2}
	e, err := engine.New(ctx, nc, slog.New(slog.NewTextHandler(io.Discard, nil)), cfg, wf)
	if err != nil {
		t.Fatal(err)
	}
	serving, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { e.Serve(serving); close(done) }()
	id, _ := e.Start(ctx, wf.Name, nil)
	r := wait(t, e, id, engine.StatusParked)
	token := r.HookWaits[0].Token
	stop()
	<-done
	stream, _ := js.Stream(ctx, "WF_STEPS")
	info, _ := stream.Info(ctx)
	config := info.Config
	config.MaxBytes = 1
	if _, err = js.UpdateStream(ctx, config); err != nil {
		t.Fatal(err)
	}
	receipt, err := e.Submit(ctx, submission(token, "offline"))
	if err != nil {
		t.Fatal(err)
	}
	h, err := e.GetHook(ctx, token)
	if err != nil || h.Status != engine.HookAccepted {
		t.Fatalf("lost input: %+v %v", h, err)
	}
	replacement, err := engine.New(ctx, nc, slog.New(slog.NewTextHandler(io.Discard, nil)), cfg, wf)
	if err != nil {
		t.Fatal(err)
	}
	go replacement.Serve(ctx)
	r = wait(t, replacement, id)
	if r.Status != engine.StatusDone {
		t.Fatal(r.Status)
	}
	again, err := replacement.Submit(ctx, submission(token, "offline"))
	if err != nil || *again != *receipt {
		t.Fatal(err)
	}
	kv, _ := js.KeyValue(ctx, "wf_runs")
	entry, _ := kv.Get(ctx, id)
	if err = kv.Purge(ctx, id, jetstream.LastRevision(entry.Revision())); err != nil {
		t.Fatal(err)
	}
	for {
		h, err = replacement.GetHook(ctx, token)
		if err != nil {
			t.Fatal(err)
		}
		if h.Status == engine.HookGone {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("cleanup not recovered")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if len(h.Payload) > 0 || h.Receipt != nil {
		t.Fatal("retained cleaned input")
	}
	if _, err = replacement.Submit(ctx, submission(token, "offline")); !errors.Is(err, engine.ErrHookGone) {
		t.Fatal(err)
	}
}

func TestHookCapacityAndSize(t *testing.T) {
	nc, js := startEmbedded(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	wf := engine.Workflow{Name: "capacity", Steps: []engine.Step{hookStep("answer", "")}}
	e, err := engine.New(ctx, nc, slog.New(slog.NewTextHandler(io.Discard, nil)), engine.Config{HookPayloadLimit: 40}, wf)
	if err != nil {
		t.Fatal(err)
	}
	go e.Serve(ctx)
	id, _ := e.Start(ctx, wf.Name, nil)
	r := wait(t, e, id, engine.StatusParked)
	s := submission(r.HookWaits[0].Token, "capacity")
	big := s
	big.Payload = []byte(`{"answer":"this answer is longer than the configured maximum","count":1}`)
	if _, err = e.Submit(ctx, big); !errors.Is(err, engine.ErrHookValidation) {
		t.Fatalf("size: %v", err)
	}
	stream, _ := js.Stream(ctx, "KV_wf_hooks")
	info, _ := stream.Info(ctx)
	cfg := info.Config
	cfg.MaxBytes = int64(info.State.Bytes) + 10
	if _, err = js.UpdateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if _, err = e.Submit(ctx, s); !errors.Is(err, engine.ErrHookCapacity) {
		t.Fatalf("capacity: %v", err)
	}
	h, err := e.GetHook(ctx, s.Token)
	if err != nil || h.Receipt != nil {
		t.Fatalf("rejected acceptance modified hook: %+v %v", h, err)
	}
	cfg.MaxBytes = 1 << 20
	js.UpdateStream(ctx, cfg)
	if _, err = e.Submit(ctx, s); err != nil {
		t.Fatal(err)
	}
	if r = wait(t, e, id); r.Status != engine.StatusDone {
		t.Fatal(r.Status)
	}
}

func TestOrderApprovalHooks(t *testing.T) {
	e, _ := newTestEngine(t, workflows.OrderPipeline())
	ctx := context.Background()
	id, err := e.Start(ctx, "order", workflows.OrderState{Order: workflows.Order{Item: "kit", Qty: 1, Amount: 750}})
	if err != nil {
		t.Fatal(err)
	}
	first := wait(t, e, id, engine.StatusParked)
	if first.HookWaits[0].Name != "fraud-review" {
		t.Fatal(first.HookWaits)
	}
	approve := func(token, id string) engine.Submission {
		return engine.Submission{Token: token, SubmissionID: id, Payload: json.RawMessage(`{"decision":"approve"}`)}
	}
	if _, err = e.Resume(ctx, id, approve(first.HookWaits[0].Token, "fraud")); err != nil {
		t.Fatal(err)
	}
	second := wait(t, e, id, engine.StatusParked)
	if len(second.HookWaits) != 2 || second.HookWaits[1].Name != "confirm-picked" {
		t.Fatal(second.HookWaits)
	}
	state, err := engine.StateOf[workflows.OrderState](second)
	if err != nil || state.Warehouse != "notified" {
		t.Fatalf("warehouse: %+v %v", state, err)
	}
	if _, err = e.Resume(ctx, id, approve(first.HookWaits[0].Token, "late")); !errors.Is(err, engine.ErrHookConflict) {
		t.Fatal(err)
	}
	if _, err = e.Resume(ctx, id, approve(second.HookWaits[1].Token, "warehouse")); err != nil {
		t.Fatal(err)
	}
	if r := wait(t, e, id); r.Status != engine.StatusDone || e.Result(r) == "" {
		t.Fatalf("order: %+v", r)
	}
}

type callbackStatus struct {
	State string `json:"state"`
}

func TestWebhookDelivery(t *testing.T) {
	nc, _ := startEmbedded(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	contract := engine.WebhookType[callbackStatus]("status-v1", func(r engine.Request) error {
		if r.Header.Get("X-Secret") != "s3cret" {
			return errors.New("bad secret")
		}
		return nil
	}, func(s callbackStatus) bool { return s.State == "done" })
	urls := make(chan string, 8)
	webhook := engine.Workflow{Name: "webhook", Steps: []engine.Step{{Name: "wait", Input: contract, Run: func(ctx context.Context, r *engine.Run) error {
		u, err := r.Webhook(ctx, "status")
		if err != nil {
			return err
		}
		urls <- u
		input, err := engine.HookInput[callbackStatus](ctx, r, "status", "")
		if err != nil {
			return err
		}
		r.State, _ = json.Marshal(input)
		return nil
	}}}}
	plain := engine.Workflow{Name: "plain", Steps: []engine.Step{hookStep("answer", "")}}
	var handler http.Handler
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handler.ServeHTTP(w, r) }))
	defer srv.Close()
	e, err := engine.New(ctx, nc, slog.New(slog.NewTextHandler(io.Discard, nil)), engine.Config{WebhookURL: srv.URL + "/hooks/"}, webhook, plain)
	if err != nil {
		t.Fatal(err)
	}
	handler = e.Webhooks()
	go e.Serve(ctx)

	id, _ := e.Start(ctx, webhook.Name, nil)
	wait(t, e, id, engine.StatusParked)
	target := <-urls
	if !strings.HasPrefix(target, srv.URL+"/hooks/") {
		t.Fatalf("webhook address %q is not under the configured URL", target)
	}
	post := func(target, secret, body string) int {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, target, strings.NewReader(body))
		req.Header.Set("X-Secret", secret)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		return res.StatusCode
	}
	for _, c := range []struct {
		name, target, secret, body string
		want                       int
	}{
		{"unsigned", target, "wrong", `{"state":"done"}`, http.StatusUnauthorized},
		{"not json", target, "s3cret", `done`, http.StatusBadRequest},
		{"too large", target, "s3cret", `{"state":"` + strings.Repeat("x", 70<<10) + `"}`, http.StatusRequestEntityTooLarge},
		{"unknown token", srv.URL + "/hooks/nope", "s3cret", `{"state":"done"}`, http.StatusNotFound},
		{"refused state", target, "s3cret", `{"state":"in_progress","added_later":1}`, http.StatusAccepted},
	} {
		if got := post(c.target, c.secret, c.body); got != c.want {
			t.Fatalf("%s: HTTP %d, want %d", c.name, got, c.want)
		}
	}
	if r, _ := e.GetRun(ctx, id); r.Status != engine.StatusParked {
		t.Fatalf("refused input resumed the run: %s", r.Status)
	}

	plainID, _ := e.Start(ctx, plain.Name, nil)
	pr := wait(t, e, plainID, engine.StatusParked)
	if got := post(srv.URL+"/hooks/"+pr.HookWaits[0].Token, "s3cret", `{"answer":"yes","count":1}`); got != http.StatusNotFound {
		t.Fatalf("plain hook reachable as a webhook: HTTP %d", got)
	}

	final := `{"state":"done","updated_at":"2026-10-15T10:00:00Z"}`
	if got := post(target, "s3cret", final); got != http.StatusAccepted {
		t.Fatalf("final state: HTTP %d", got)
	}
	r := wait(t, e, id, engine.StatusDone)
	if string(r.State) != `{"state":"done"}` {
		t.Fatalf("run state %s", r.State)
	}
	for _, body := range []string{final, `{"state":"published"}`, `{"state":"done","updated_at":"later"}`} {
		if got := post(target, "s3cret", body); got != http.StatusAccepted {
			t.Fatalf("late call %s: HTTP %d", body, got)
		}
	}
	req, _ := http.NewRequest(http.MethodGet, target, nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil || res.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET: %v %v", res.StatusCode, err)
	}
	res.Body.Close()
}

type delivery struct {
	Event      string `json:"event"`
	DeliveryID string `json:"delivery_id"`
	Project    int    `json:"project"`
	Seen       bool   `json:"seen,omitempty"`
}

var deliveryContract = engine.WebhookType[delivery]("delivery-v1", func(r engine.Request) error {
	if r.Header.Get("X-Secret") != "s3cret" {
		return errors.New("bad secret")
	}
	return nil
}, func(d delivery) bool { return d.Event != "progress" })

func deliveryKey(body []byte) (string, error) {
	var d delivery
	err := json.Unmarshal(body, &d)
	return d.DeliveryID, err
}

var inbox = engine.Workflow{Name: "inbox", Steps: []engine.Step{{Name: "route", Run: engine.Typed(func(ctx context.Context, r *engine.Run, d *delivery) error {
	d.Seen = true
	return nil
})}}}

func inboxTrigger(token string) engine.Trigger {
	return engine.Trigger{Token: token, Workflow: inbox.Name, Input: deliveryContract, Key: deliveryKey}
}

func triggerServer(t *testing.T, e *engine.Engine) func(token, secret, body string) int {
	t.Helper()
	srv := httptest.NewServer(e.Webhooks())
	t.Cleanup(srv.Close)
	return func(token, secret, body string) int {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/hooks/"+token, strings.NewReader(body))
		req.Header.Set("X-Secret", secret)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		return res.StatusCode
	}
}

func runsOf(t *testing.T, e *engine.Engine, workflow string) []*engine.Run {
	t.Helper()
	runs, err := e.ListRuns(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var of []*engine.Run
	for _, r := range runs {
		if r.Workflow == workflow {
			of = append(of, r)
		}
	}
	return of
}

func TestTriggerStartsOneRunPerDelivery(t *testing.T) {
	nc, _ := startEmbedded(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	taken := engine.Workflow{Name: "taken", Steps: []engine.Step{hookStep("answer", "inbox-old")}}
	e, err := engine.New(ctx, nc, slog.New(slog.NewTextHandler(io.Discard, nil)), engine.Config{
		BackOff: []time.Duration{50 * time.Millisecond}, MaxDeliver: 2,
		Triggers: []engine.Trigger{inboxTrigger("inbox-old"), inboxTrigger("inbox-new")},
	}, inbox, taken)
	if err != nil {
		t.Fatal(err)
	}
	go e.Serve(ctx)
	post := triggerServer(t, e)

	first := `{"event":"ready","delivery_id":"01M4QGYFNGAY47SX2N1D0K3JCC","project":16710`
	for _, c := range []struct {
		name, token, secret, body string
		want                      int
	}{
		{"unsigned", "inbox-new", "wrong", first + "}", http.StatusUnauthorized},
		{"not an object", "inbox-new", "s3cret", `[1,2]`, http.StatusBadRequest},
		{"null", "inbox-new", "s3cret", `null`, http.StatusBadRequest},
		{"no delivery id", "inbox-new", "s3cret", `{"event":"ready","project":1}`, http.StatusBadRequest},
		{"declined", "inbox-new", "s3cret", `{"event":"progress","delivery_id":"p1"}`, http.StatusAccepted},
		{"too large", "inbox-new", "s3cret", first + `,"pad":"` + strings.Repeat("x", 70<<10) + `"}`, http.StatusRequestEntityTooLarge},
		{"unknown token", "inbox-nope", "s3cret", first + "}", http.StatusNotFound},
	} {
		if got := post(c.token, c.secret, c.body); got != c.want {
			t.Fatalf("%s: HTTP %d, want %d", c.name, got, c.want)
		}
	}
	if runs := runsOf(t, e, inbox.Name); len(runs) != 0 {
		t.Fatalf("rejected calls started %d runs", len(runs))
	}

	codes := make([]int, 20)
	var wg sync.WaitGroup
	for i := range codes {
		wg.Go(func() { codes[i] = post("inbox-new", "s3cret", fmt.Sprintf(`%s,"sent_at":"%d"}`, first, i)) })
	}
	wg.Wait()
	for i, code := range codes {
		if code != http.StatusAccepted {
			t.Fatalf("retry %d of one delivery: HTTP %d", i, code)
		}
	}
	runs := runsOf(t, e, inbox.Name)
	if len(runs) != 1 {
		t.Fatalf("20 retries of one delivery started %d runs", len(runs))
	}
	d, err := engine.StateOf[delivery](wait(t, e, runs[0].ID, engine.StatusDone))
	if err != nil || !d.Seen || d.Project != 16710 {
		t.Fatalf("run state %+v, %v", d, err)
	}

	if got := post("inbox-new", "s3cret", `{"event":"failed","delivery_id":"second","project":16710}`); got != http.StatusAccepted {
		t.Fatalf("second delivery: HTTP %d", got)
	}
	if got := post("inbox-old", "s3cret", first+"}"); got != http.StatusAccepted {
		t.Fatalf("first delivery on the old token: HTTP %d", got)
	}
	if runs := runsOf(t, e, inbox.Name); len(runs) != 3 {
		t.Fatalf("a new delivery and a second token should each start a run, got %d runs", len(runs))
	}

	if _, err := e.Submit(ctx, engine.Submission{Token: "inbox-new", SubmissionID: "x", Payload: json.RawMessage(`{}`)}); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("Submit to a trigger token: %v", err)
	}
	id, _ := e.Start(ctx, taken.Name, nil)
	if r := wait(t, e, id, engine.StatusFailed); !strings.Contains(r.Error, engine.ErrHookConflict.Error()) {
		t.Fatalf("a Hook took a trigger token: %+v", r)
	}
}

func TestTriggerTokenReservation(t *testing.T) {
	nc, _ := startEmbedded(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	plain := engine.Workflow{Name: "plain", Steps: []engine.Step{hookStep("answer", "held-by-hook")}}
	e, err := engine.New(ctx, nc, discard, engine.Config{Triggers: []engine.Trigger{inboxTrigger("retired")}}, inbox, plain)
	if err != nil {
		t.Fatal(err)
	}
	go e.Serve(ctx)
	id, _ := e.Start(ctx, plain.Name, nil)
	wait(t, e, id, engine.StatusParked)

	if _, err := engine.New(ctx, nc, discard, engine.Config{Triggers: []engine.Trigger{inboxTrigger("held-by-hook")}}, inbox, plain); err == nil || !strings.Contains(err.Error(), "already a hook token") {
		t.Fatalf("trigger over a Hook token: %v", err)
	}
	moved := engine.Trigger{Token: "retired", Workflow: plain.Name, Input: deliveryContract}
	if _, err := engine.New(ctx, nc, discard, engine.Config{Triggers: []engine.Trigger{moved}}, inbox, plain); err == nil || !strings.Contains(err.Error(), "reserved for") {
		t.Fatalf("trigger token moved to another workflow: %v", err)
	}
	if _, err := engine.New(ctx, nc, discard, engine.Config{Triggers: []engine.Trigger{{Token: "has/slash", Workflow: inbox.Name, Input: deliveryContract}}}, inbox, plain); err == nil {
		t.Fatal("trigger token with a slash accepted")
	}

	without, err := engine.New(ctx, nc, discard, engine.Config{}, inbox, plain)
	if err != nil {
		t.Fatal(err)
	}
	if got := triggerServer(t, without)("retired", "s3cret", `{"event":"ready","delivery_id":"late"}`); got != http.StatusAccepted {
		t.Fatalf("removed trigger: HTTP %d, want 202", got)
	}
	if runs := runsOf(t, without, inbox.Name); len(runs) != 0 {
		t.Fatalf("removed trigger started %d runs", len(runs))
	}
	if r, _ := e.GetRun(ctx, id); r.Status != engine.StatusParked {
		t.Fatalf("Hook holder changed: %s", r.Status)
	}
}

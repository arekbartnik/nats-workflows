package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
	e, err := engine.New(ctx, nc, slog.New(slog.NewTextHandler(io.Discard, nil)), engine.Config{}, wf)
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

package engine

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func protocolEngine(t *testing.T) *Engine {
	t.Helper()
	ns, err := server.NewServer(&server.Options{DontListen: true, JetStream: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("server not ready")
	}
	nc, err := nats.Connect("", nats.InProcessServer(ns))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close(); ns.Shutdown(); ns.WaitForShutdown() })
	e, err := New(context.Background(), nc, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{}, Workflow{Name: "protocol", Steps: []Step{{Name: "wait", Input: HookType[map[string]any]("v1", nil), Run: func(context.Context, *Run) error { return ErrPark }}}})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestHookCleanupRacesRetry(t *testing.T) {
	e := protocolEngine(t)
	ctx := context.Background()
	for range 20 {
		id, err := e.Start(ctx, "protocol", nil)
		if err != nil {
			t.Fatal(err)
		}
		r, _ := e.GetRun(ctx, id)
		r.engine = e
		h, err := r.RegisterHook(ctx, "wait", "")
		if err != nil {
			t.Fatal(err)
		}
		submission := Submission{Token: h.Token, SubmissionID: "accepted", Payload: json.RawMessage(`{"input":"kept"}`)}
		if _, err = e.Submit(ctx, submission); err != nil {
			t.Fatal(err)
		}
		r, _ = e.GetRun(ctx, id)
		r.Status = StatusFailed
		past := time.Now().Add(-time.Hour)
		r.ExpiresAt = &past
		if err = e.putRun(ctx, r); err != nil {
			t.Fatal(err)
		}
		var retryErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); retryErr = e.Retry(ctx, id) }()
		go func() { defer wg.Done(); e.maintainRuns(ctx) }()
		wg.Wait()
		e.cleanHooks(ctx)
		h, err = e.GetHook(ctx, h.Token)
		if err != nil {
			t.Fatal(err)
		}
		if retryErr == nil {
			r, err = e.GetRun(ctx, id)
			if err != nil || r.Status != StatusRunning || r.ExpiresAt != nil {
				t.Fatalf("retried Run lost: %+v %v", r, err)
			}
			if h.Receipt == nil || h.Status == "gone" || len(h.Payload) == 0 {
				t.Fatal("cleanup stripped retried input")
			}
		} else {
			if _, err = e.GetRun(ctx, id); !errors.Is(err, jetstream.ErrKeyNotFound) {
				t.Fatalf("neither retry nor cleanup won: %v", err)
			}
			if h.Status != "gone" || h.Receipt != nil || len(h.Payload) != 0 {
				t.Fatalf("cleanup lost: %+v", h)
			}
		}
	}
}

func TestConcurrentHookRegistration(t *testing.T) {
	e := protocolEngine(t)
	ctx := context.Background()
	id, _ := e.Start(ctx, "protocol", nil)
	r1, _ := e.GetRun(ctx, id)
	r2, _ := e.GetRun(ctx, id)
	r1.engine = e
	r2.engine = e
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = r1.RegisterHook(ctx, "wait", "") }()
	go func() { defer wg.Done(); _, _ = r2.RegisterHook(ctx, "wait", "") }()
	wg.Wait()
	r, _ := e.GetRun(ctx, id)
	r.engine = e
	h, err := r.RegisterHook(ctx, "wait", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.HookWaits) != 1 || r.HookWaits[0].Token != h.Token {
		t.Fatal("registration retry changed token")
	}
	if _, err = r.RegisterHook(ctx, "wait", "different"); !errors.Is(err, ErrHookConflict) {
		t.Fatal(err)
	}
	if _, err = r.RegisterHook(ctx, "second", ""); !errors.Is(err, ErrHookConflict) {
		t.Fatal(err)
	}
	// Registering a different contract under the same name cannot reinterpret input.
	e.workflows["protocol"].Steps[0].Input = HookType[map[string]any]("v2", nil)
	if _, err = r.RegisterHook(ctx, "wait", ""); !errors.Is(err, ErrHookConflict) {
		t.Fatal(err)
	}
}

func TestHookConsumptionRecoveryAndFullStoreCleanup(t *testing.T) {
	e := protocolEngine(t)
	ctx := context.Background()
	id, _ := e.Start(ctx, "protocol", nil)
	r, _ := e.GetRun(ctx, id)
	r.engine = e
	h, err := r.RegisterHook(ctx, "wait", "permanent")
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.Submit(ctx, Submission{Token: h.Token, SubmissionID: "one", Payload: json.RawMessage(`{"value":"kept"}`)})
	if err != nil {
		t.Fatal(err)
	}
	// Reproduce a successful Step CAS before the Hook status update.
	r, _ = e.GetRun(ctx, id)
	r.HookWaits[0].Consumed = "one"
	r.Step = 1
	r.Status = StatusDone
	if err = e.putRun(ctx, r); err != nil {
		t.Fatal(err)
	}
	e.reconcileRunHooks(ctx, r)
	h, _ = e.GetHook(ctx, h.Token)
	if h.Status != "consumed" {
		t.Fatal(h.Status)
	}
	stream, _ := e.js.Stream(ctx, "KV_wf_hooks")
	info, _ := stream.Info(ctx)
	cfg := info.Config
	cfg.MaxBytes = int64(info.State.Bytes)
	if _, err = e.js.UpdateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if err = e.kv.Purge(ctx, id, jetstream.LastRevision(r.rev)); err != nil {
		t.Fatal(err)
	}
	e.cleanHooks(ctx)
	h, _ = e.GetHook(ctx, h.Token)
	if h.Status != "gone" {
		t.Fatalf("full store prevented shrinking cleanup: %+v", h)
	}
	// The reservation remains even when another Run requests this token.
	id, _ = e.Start(ctx, "protocol", nil)
	r, _ = e.GetRun(ctx, id)
	r.engine = e
	if _, err = r.RegisterHook(ctx, "wait", "permanent"); !errors.Is(err, ErrHookConflict) {
		t.Fatal(err)
	}
}

func TestEquivalentJSONNumbers(t *testing.T) {
	for _, pair := range [][2]string{{`1`, `1.0`}, {`100`, `1e2`}, {`-0`, `0`}, {`0.0010`, `1e-3`}, {`1e10000000000`, `10e9999999999`}} {
		if !equivalentJSON([]byte(pair[0]), []byte(pair[1])) {
			t.Fatal(pair)
		}
	}
	if equivalentJSON([]byte(`9007199254740992`), []byte(`9007199254740993`)) {
		t.Fatal("rounded distinct integers")
	}
}

func TestStaleWakeupAfterDedupWindow(t *testing.T) {
	e := protocolEngine(t)
	ctx := context.Background()
	id, _ := e.Start(ctx, "protocol", nil)
	r, _ := e.GetRun(ctx, id)
	oldExecution := r.Execution
	// Consume the initial delivery as though it parked, then commit a new wakeup.
	msg, err := e.cons.Next(jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err = msg.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	r.Execution = "new-wakeup"
	if err = e.putRun(ctx, r); err != nil {
		t.Fatal(err)
	}
	stream, _ := e.js.Stream(ctx, "WF_STEPS")
	info, _ := stream.Info(ctx)
	cfg := info.Config
	cfg.Duplicates = 100 * time.Millisecond
	if _, err = e.js.UpdateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	stale := *r
	stale.Execution = oldExecution
	if err = e.enqueue(ctx, &stale, 0); err != nil {
		t.Fatal(err)
	}
	msg, err = e.cons.Next(jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	called := false
	e.workflows["protocol"].Steps[0].Run = func(context.Context, *Run) error { called = true; return nil }
	e.handle(ctx, msg)
	if called {
		t.Fatal("stale same-Step generation executed after dedup expired")
	}
	if err = e.enqueue(ctx, r, 0); err != nil {
		t.Fatal(err)
	}
	msg, err = e.cons.Next(jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	e.handle(ctx, msg)
	if !called {
		t.Fatal("current generation did not execute")
	}
}

func TestConcurrentTokenOwners(t *testing.T) {
	e := protocolEngine(t)
	ctx := context.Background()
	var runs []*Run
	for range 2 {
		id, _ := e.Start(ctx, "protocol", nil)
		r, _ := e.GetRun(ctx, id)
		r.engine = e
		runs = append(runs, r)
	}
	results := make(chan error, 2)
	for _, r := range runs {
		go func() { _, err := r.RegisterHook(ctx, "wait", "shared-token"); results <- err }()
	}
	wins := 0
	for range 2 {
		err := <-results
		if err == nil {
			wins++
		} else if !errors.Is(err, ErrHookConflict) {
			t.Fatal(err)
		}
	}
	if wins != 1 {
		t.Fatalf("token owners: %d", wins)
	}
}

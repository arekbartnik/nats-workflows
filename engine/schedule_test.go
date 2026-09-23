package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arekbartnik/nats-workflows/engine"
	"github.com/nats-io/nats.go/jetstream"
)

func TestScheduledRuns(t *testing.T) {
	var executed atomic.Int32
	started := make(chan time.Time, 10)
	wf := engine.Workflow{Name: "scheduled", Steps: []engine.Step{{Name: "record", Run: func(context.Context, *engine.Run) error {
		executed.Add(1)
		started <- time.Now()
		return nil
	}}}}
	eng, _ := newTestEngine(t, wf)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	at := time.Now().Add(450 * time.Millisecond)
	id, err := eng.Schedule(ctx, wf.Name, map[string]string{"value": "kept"}, at)
	if err != nil {
		t.Fatal(err)
	}
	run, err := eng.GetRun(ctx, id)
	if err != nil || run.Status != engine.StatusScheduled || !run.StartAt.Equal(at) {
		t.Fatalf("pending: %+v, %v", run, err)
	}
	run, err = eng.Wait(ctx, id)
	if err != nil || run.Status != engine.StatusDone {
		t.Fatalf("finished: %+v, %v", run, err)
	}
	if when := <-started; when.Before(at) {
		t.Fatalf("started early: %s < %s", when, at)
	}
	if run.ExpiresAt == nil || string(run.State) != `{"value":"kept"}` {
		t.Fatalf("state/retention: %+v", run)
	}
	if err := eng.Cancel(ctx, id); !errors.Is(err, engine.ErrConflict) {
		t.Fatalf("cancel completed: %v", err)
	}
	if _, err := eng.Schedule(ctx, wf.Name, nil, time.Now().Add(-time.Second)); !errors.Is(err, engine.ErrStartTime) {
		t.Fatal(err)
	}

	id, err = eng.Schedule(ctx, wf.Name, nil, time.Now().Add(30*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	run, err = eng.Wait(ctx, id)
	if err != nil || run.Status != engine.StatusCancelled || run.ExpiresAt == nil {
		t.Fatalf("cancelled: %+v, %v", run, err)
	}
	if err := eng.Retry(ctx, id); err == nil {
		t.Fatal("retried cancelled run")
	}
	if executed.Load() != 1 {
		t.Fatal("cancelled run executed")
	}
}

func TestRescheduleRejectsOldDelivery(t *testing.T) {
	started := make(chan time.Time, 1)
	wf := engine.Workflow{Name: "reschedule", Steps: []engine.Step{{Name: "record", Run: func(context.Context, *engine.Run) error { started <- time.Now(); return nil }}}}
	eng, js := newTestEngine(t, wf)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	id, err := eng.Schedule(ctx, wf.Name, nil, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	kv, _ := js.KeyValue(ctx, "wf_runs")
	entry, _ := kv.Get(ctx, id)
	at := time.Now().Add(600 * time.Millisecond)
	if err := eng.Reschedule(ctx, id, at); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(map[string]any{"run_id": id, "generation": entry.Revision()})
	if _, err := js.Publish(ctx, "wf.start."+id, data); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Wait(ctx, id); err != nil {
		t.Fatal(err)
	}
	if when := <-started; when.Before(at) {
		t.Fatalf("old delivery started rescheduled run at %s", when)
	}
}

func TestScheduleRecoveryAndRetention(t *testing.T) {
	nc, js := startEmbedded(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	wf := engine.Workflow{Name: "recover", Steps: []engine.Step{{Name: "done", Run: func(context.Context, *engine.Run) error { return nil }}}}
	eng, err := engine.New(ctx, nc, slog.New(slog.NewTextHandler(io.Discard, nil)), engine.Config{}, wf)
	if err != nil {
		t.Fatal(err)
	}
	kv, _ := js.KeyValue(ctx, "wf_runs")
	// Simulate a crash after saving intent but before publishing any schedule.
	at := time.Now().Add(-time.Hour)
	data, _ := json.Marshal(engine.Run{ID: "missed", Workflow: wf.Name, Status: engine.StatusScheduled, StartAt: &at})
	if _, err := kv.Create(ctx, "missed", data); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour)
	data, _ = json.Marshal(engine.Run{ID: "expired", Workflow: wf.Name, Status: engine.StatusCancelled, ExpiresAt: &old})
	kv.Create(ctx, "expired", data)
	data, _ = json.Marshal(engine.Run{ID: "parked", Workflow: wf.Name, Status: engine.StatusParked})
	kv.Create(ctx, "parked", data)
	go eng.Serve(ctx)
	if run, err := eng.Wait(ctx, "missed"); err != nil || run.Status != engine.StatusDone {
		t.Fatalf("recovery: %+v %v", run, err)
	}
	for {
		_, err := kv.Get(ctx, "expired")
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("terminal run never expired")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if _, err := eng.GetRun(ctx, "parked"); err != nil {
		t.Fatal(err)
	}
	status, err := kv.Status(ctx)
	if err != nil || status.TTL() != 0 {
		t.Fatalf("bucket still expires live runs: %v", err)
	}
}

func TestCancelVersusStart(t *testing.T) {
	var executed atomic.Int32
	wf := engine.Workflow{Name: "race", Steps: []engine.Step{{Name: "done", Run: func(context.Context, *engine.Run) error { executed.Add(1); return nil }}}}
	eng, js := newTestEngine(t, wf)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	kv, _ := js.KeyValue(ctx, "wf_runs")
	for i := 0; i < 15; i++ {
		id, err := eng.Schedule(ctx, wf.Name, nil, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		entry, _ := kv.Get(ctx, id)
		var run engine.Run
		json.Unmarshal(entry.Value(), &run)
		past := time.Now().Add(-time.Second)
		run.StartAt = &past
		data, _ := json.Marshal(run)
		rev, err := kv.Update(ctx, id, data, entry.Revision())
		if err != nil {
			t.Fatal(err)
		}
		data, _ = json.Marshal(map[string]any{"run_id": id, "generation": rev})
		before := executed.Load()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); js.Publish(ctx, "wf.start."+id, data) }()
		err = eng.Cancel(ctx, id)
		wg.Wait()
		result, waitErr := eng.Wait(ctx, id)
		if waitErr != nil {
			t.Fatal(waitErr)
		}
		if err == nil && (result.Status != engine.StatusCancelled || executed.Load() != before) {
			t.Fatal("successful cancellation executed a step")
		}
		if err != nil && !errors.Is(err, engine.ErrConflict) {
			t.Fatal(err)
		}
	}
}

func TestStartClaimRecovery(t *testing.T) {
	nc, js := startEmbedded(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	wf := engine.Workflow{Name: "claim", Steps: []engine.Step{{Name: "done", Run: func(context.Context, *engine.Run) error { return nil }}}}
	eng, err := engine.New(ctx, nc, slog.New(slog.NewTextHandler(io.Discard, nil)), engine.Config{}, wf)
	if err != nil {
		t.Fatal(err)
	}
	kv, _ := js.KeyValue(ctx, "wf_runs")
	// A prior starter saved running, then crashed before enqueue. Its durable
	// start delivery remains available to the replacement engine.
	run := engine.Run{ID: "claimed", Workflow: wf.Name, Status: engine.StatusRunning, StartGeneration: 123}
	data, _ := json.Marshal(run)
	if _, err := kv.Create(ctx, run.ID, data); err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(ctx, "wf.start.claimed", []byte(`{"run_id":"claimed","generation":123}`)); err != nil {
		t.Fatal(err)
	}
	go eng.Serve(ctx)
	if result, err := eng.Wait(ctx, run.ID); err != nil || result.Status != engine.StatusDone {
		t.Fatalf("claim recovery: %+v %v", result, err)
	}
}

func TestRetryClearsExpiry(t *testing.T) {
	var attempts atomic.Int32
	wf := engine.Workflow{Name: "retry-expiry", Steps: []engine.Step{{Name: "retry", Run: func(context.Context, *engine.Run) error {
		if attempts.Add(1) == 1 {
			return engine.ErrTerminal
		}
		return engine.ErrPark
	}}}}
	eng, _ := newTestEngine(t, wf)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	id, err := eng.Start(ctx, wf.Name, nil)
	if err != nil {
		t.Fatal(err)
	}
	run, err := eng.Wait(ctx, id)
	if err != nil || run.ExpiresAt == nil {
		t.Fatalf("failed expiry: %+v %v", run, err)
	}
	if err := eng.Retry(ctx, id); err != nil {
		t.Fatal(err)
	}
	run, err = eng.Wait(ctx, id, engine.StatusParked)
	if err != nil || run.ExpiresAt != nil {
		t.Fatalf("retry retained expiry: %+v %v", run, err)
	}
}

func TestNativeScheduleWithoutExecutor(t *testing.T) {
	nc, js := startEmbedded(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	wf := engine.Workflow{Name: "offline", Steps: []engine.Step{{Name: "done", Run: func(context.Context, *engine.Run) error { return nil }}}}
	eng, err := engine.New(ctx, nc, slog.New(slog.NewTextHandler(io.Discard, nil)), engine.Config{}, wf)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().Add(350 * time.Millisecond)
	id, err := eng.Schedule(ctx, wf.Name, nil, at)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(ctx, "WF_SCHEDULES")
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := stream.Consumer(ctx, "starter")
	if err != nil {
		t.Fatal(err)
	}
	// No Serve or maintenance is running: only NATS can deliver this start.
	for {
		info, err := consumer.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.NumPending > 0 {
			if time.Now().Before(at) {
				t.Fatal("native schedule delivered early")
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("NATS never delivered scheduled start")
		case <-time.After(10 * time.Millisecond):
		}
	}
	run, err := eng.GetRun(ctx, id)
	if err != nil || run.Status != engine.StatusScheduled {
		t.Fatalf("offline run: %+v %v", run, err)
	}
	go eng.Serve(ctx)
	if run, err := eng.Wait(ctx, id); err != nil || run.Status != engine.StatusDone {
		t.Fatalf("overdue execution: %+v %v", run, err)
	}
}

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/arekbartnik/nats-workflows/engine"
	"github.com/nats-io/nats.go/jetstream"
)

func serveForTest(t *testing.T, eng *engine.Engine) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- eng.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Serve did not stop")
		}
	})
}

func TestNamespacesIsolateRunsHooksAndSchedules(t *testing.T) {
	nc, _ := startEmbedded(t)
	ctx := context.Background()
	type state struct {
		Owner string `json:"owner"`
	}
	type input struct {
		Value string `json:"value"`
	}
	var engines []*engine.Engine
	var ids []string
	namespaces := []string{"", "app", "app_b"}
	for _, namespace := range namespaces {
		eng, err := engine.New(ctx, nc, nil, engine.Config{Namespace: namespace},
			engine.Workflow{Name: "approval", Steps: []engine.Step{{
				Name: "wait", Input: engine.HookType[input]("v1", nil),
				Run: engine.Typed(func(ctx context.Context, r *engine.Run, s *state) error {
					in, err := engine.HookInput[input](ctx, r, "approval", "same-token")
					if err != nil {
						return err
					}
					s.Owner = in.Value
					return nil
				}),
			}}},
			engine.Workflow{Name: "scheduled", Steps: []engine.Step{{
				Name: "record", Run: engine.Typed(func(ctx context.Context, r *engine.Run, s *state) error {
					s.Owner = namespace
					return nil
				}),
			}}},
		)
		if err != nil {
			t.Fatal(err)
		}
		engines = append(engines, eng)
		serveForTest(t, eng)
		id, err := eng.Start(ctx, "approval", nil)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
		wait(t, eng, id, engine.StatusParked)
	}
	for i, eng := range engines {
		runs, err := eng.ListRuns(ctx)
		if err != nil || len(runs) != 1 || runs[0].ID != ids[i] {
			t.Fatalf("namespace %q leaked runs: %v, %v", namespaces[i], runs, err)
		}
		if _, err := eng.GetRun(ctx, ids[(i+1)%len(ids)]); !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatalf("read another namespace's run: %v", err)
		}
		_, err = eng.Submit(ctx, engine.Submission{
			Token: "same-token", SubmissionID: "same-submission",
			Payload: []byte(fmt.Sprintf(`{"value":%q}`, namespaces[i])),
		})
		if err != nil {
			t.Fatal(err)
		}
		run := wait(t, eng, ids[i], engine.StatusDone)
		s, err := engine.StateOf[state](run)
		if err != nil || s.Owner != namespaces[i] {
			t.Fatalf("namespace %q received wrong Hook input: %+v, %v", namespaces[i], s, err)
		}
	}
	var scheduled []string
	for _, eng := range engines {
		id, err := eng.Schedule(ctx, "scheduled", nil, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		scheduled = append(scheduled, id)
		if err := eng.Reschedule(ctx, id, time.Now().Add(250*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		cancelled, err := eng.Schedule(ctx, "scheduled", nil, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		if err := eng.Cancel(ctx, cancelled); err != nil {
			t.Fatal(err)
		}
		run, err := eng.GetRun(ctx, cancelled)
		if err != nil || run.Status != engine.StatusCancelled {
			t.Fatalf("cancelled run: %v, %v", run, err)
		}
	}
	for i, eng := range engines {
		run := wait(t, eng, scheduled[i], engine.StatusDone)
		s, err := engine.StateOf[state](run)
		if err != nil || s.Owner != namespaces[i] {
			t.Fatalf("namespace %q executed in the wrong worker: %+v, %v", namespaces[i], s, err)
		}
	}
}

func TestNewRejectsInvalidDefinitionsBeforeCreatingResources(t *testing.T) {
	nc, js := startEmbedded(t)
	ctx := context.Background()
	valid := engine.Workflow{Name: "wf", Steps: []engine.Step{appendStep("done")}}
	for _, tc := range []struct {
		name string
		cfg  engine.Config
		wfs  []engine.Workflow
	}{
		{name: "wildcard namespace", cfg: engine.Config{Namespace: "app.>"}, wfs: []engine.Workflow{valid}},
		{name: "empty workflow name", wfs: []engine.Workflow{{Steps: valid.Steps}}},
		{name: "empty workflow", wfs: []engine.Workflow{{Name: "empty"}}},
		{name: "duplicate workflow", wfs: []engine.Workflow{valid, valid}},
		{name: "missing step function", wfs: []engine.Workflow{{Name: "wf", Steps: []engine.Step{{Name: "broken"}}}}},
		{name: "duplicate step", wfs: []engine.Workflow{{Name: "wf", Steps: []engine.Step{appendStep("x"), appendStep("x")}}}},
		{name: "invalid input contract", wfs: []engine.Workflow{{Name: "wf", Steps: []engine.Step{{Name: "x", Run: valid.Steps[0].Run, Input: &engine.InputContract{ID: "invalid"}}}}}},
		{name: "invalid backoff", cfg: engine.Config{BackOff: []time.Duration{-time.Second}}, wfs: []engine.Workflow{valid}},
		{name: "invalid delivery budget", cfg: engine.Config{MaxDeliver: 1}, wfs: []engine.Workflow{valid}},
		{name: "invalid hook limit", cfg: engine.Config{HookMaxBytes: -1}, wfs: []engine.Workflow{valid}},
		{name: "invalid step limit", cfg: engine.Config{StepMaxBytes: -1}, wfs: []engine.Workflow{valid}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := engine.New(ctx, nc, nil, tc.cfg, tc.wfs...); err == nil {
				t.Fatal("accepted invalid configuration")
			}
			if _, err := js.Stream(ctx, "WF_STEPS"); !errors.Is(err, jetstream.ErrStreamNotFound) {
				t.Fatalf("created resources before validation: %v", err)
			}
		})
	}
}

func TestWorkflowDefinitionsAreCopied(t *testing.T) {
	nc, _ := startEmbedded(t)
	wf := engine.Workflow{Name: "wf", Steps: []engine.Step{appendStep("original")}}
	eng, err := engine.New(context.Background(), nc, nil, engine.Config{}, wf)
	if err != nil {
		t.Fatal(err)
	}
	wf.Steps[0] = appendStep("changed by caller")
	eng.Workflows()[0].Steps[0] = appendStep("changed through listing")
	serveForTest(t, eng)
	id, err := eng.Start(context.Background(), "wf", nil)
	if err != nil {
		t.Fatal(err)
	}
	s := stateOf(t, wait(t, eng, id, engine.StatusDone))
	if len(s.Log) != 1 || s.Log[0] != "original" {
		t.Fatalf("caller changed registered workflow: %+v", s)
	}
}

func TestServeWaitsForCallbacksAndLeavesConnectionOpen(t *testing.T) {
	nc, _ := startEmbedded(t)
	started, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	eng, err := engine.New(context.Background(), nc, nil, engine.Config{}, engine.Workflow{
		Name: "wf", Steps: []engine.Step{{Name: "active", Run: func(ctx context.Context, r *engine.Run) error {
			close(started)
			<-ctx.Done()
			close(cancelled)
			<-release
			return ctx.Err()
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- eng.Serve(ctx) }()
	t.Cleanup(cancel)
	defer close(release)
	if _, err := eng.Start(ctx, "wf", nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("step did not start")
	}
	if err := eng.Serve(ctx); !errors.Is(err, engine.ErrServing) {
		t.Fatalf("concurrent Serve: %v", err)
	}
	cancel()
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("step context was not cancelled")
	}
	select {
	case err := <-done:
		t.Fatalf("Serve returned before step cleanup: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	release <- struct{}{}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not finish after callback returned")
	}
	if nc.IsClosed() || nc.Flush() != nil {
		t.Fatal("Serve closed the caller's connection")
	}
	if err := eng.Serve(ctx); err != nil {
		t.Fatalf("Serve did not release its lifecycle state: %v", err)
	}
}

func TestServeReportsClosedConnection(t *testing.T) {
	nc, _ := startEmbedded(t)
	eng, err := engine.New(context.Background(), nc, nil, engine.Config{}, engine.Workflow{Name: "wf", Steps: []engine.Step{appendStep("done")}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- eng.Serve(ctx) }()
	id, err := eng.Start(ctx, "wf", nil)
	if err != nil {
		t.Fatal(err)
	}
	wait(t, eng, id, engine.StatusDone)
	nc.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Serve did not report connection closure")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not report stopped consumers")
	}
}

package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arekbartnik/nats-workflows/engine"
)

func startEmbedded(t *testing.T) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	ns, err := server.NewServer(&server.Options{
		ServerName: "engine-test",
		DontListen: true,
		JetStream:  true,
		StoreDir:   t.TempDir(),
	})
	if err != nil {
		t.Fatalf("creating embedded server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("embedded server never became ready")
	}
	nc, err := nats.Connect("", nats.InProcessServer(ns))
	if err != nil {
		t.Fatalf("connecting in-process: %v", err)
	}
	t.Cleanup(func() {
		nc.Close()
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	return nc, js
}

func newTestEngine(t *testing.T, wfs ...engine.Workflow) (*engine.Engine, jetstream.JetStream) {
	t.Helper()
	nc, js := startEmbedded(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	eng, err := engine.New(ctx, nc, slog.New(slog.NewTextHandler(io.Discard, nil)),
		engine.Config{
			BackOff:    []time.Duration{50 * time.Millisecond, 50 * time.Millisecond},
			MaxDeliver: 3,
		}, wfs...)
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	go eng.Serve(ctx)
	return eng, js
}

type tState struct {
	Log      []string `json:"log,omitempty"`
	Kept     string   `json:"kept,omitempty"`
	Decision string   `json:"decision,omitempty"`
	Amount   int      `json:"amount,omitempty"`
}

func appendStep(name string) engine.Step {
	return engine.Step{Name: name, Run: engine.Typed(func(ctx context.Context, run *engine.Run, s *tState) error {
		s.Log = append(s.Log, name)
		return nil
	})}
}

func wait(t *testing.T, eng *engine.Engine, id string, statuses ...engine.Status) *engine.Run {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	run, err := eng.Wait(ctx, id, statuses...)
	if err != nil {
		t.Fatalf("waiting for %v: %v", statuses, err)
	}
	return run
}

func stateOf(t *testing.T, run *engine.Run) tState {
	t.Helper()
	s, err := engine.StateOf[tState](run)
	if err != nil {
		t.Fatalf("decoding state: %v", err)
	}
	return s
}

func TestHappyPath(t *testing.T) {
	eng, _ := newTestEngine(t, engine.Workflow{
		Name:  "wf",
		Steps: []engine.Step{appendStep("s0"), appendStep("s1"), appendStep("s2")},
		Result: func(run *engine.Run) string {
			s, _ := engine.StateOf[tState](run)
			return strings.Join(s.Log, ",")
		},
	})
	id, err := eng.Start(context.Background(), "wf", nil)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	run := wait(t, eng, id)
	if run.Status != engine.StatusDone {
		t.Fatalf("status = %q, want done", run.Status)
	}
	if got := eng.Result(run); got != "s0,s1,s2" {
		t.Fatalf("Result = %q, want s0,s1,s2", got)
	}
}

func TestStartWithIDStartsOnceAndAtOnce(t *testing.T) {
	eng, _ := newTestEngine(t, engine.Workflow{Name: "wf", Steps: []engine.Step{appendStep("s0")}})
	ctx := context.Background()
	if _, err := eng.StartWithID(ctx, "order-7", "other", nil); err == nil {
		t.Fatal("started an unknown workflow")
	}
	if _, err := eng.StartWithID(ctx, "", "wf", nil); err == nil {
		t.Fatal("started a run without an id")
	}
	before := time.Now()
	run, err := eng.StartWithID(ctx, "order-7", "wf", tState{Kept: "first"})
	if err != nil {
		t.Fatalf("StartWithID: %v", err)
	}
	if run.ID != "order-7" || run.Status != engine.StatusRunning {
		t.Fatalf("run = %+v", run)
	}
	again, err := eng.StartWithID(ctx, "order-7", "wf", tState{Kept: "second"})
	if err != nil {
		t.Fatalf("second StartWithID: %v", err)
	}
	if again.ID != run.ID || stateOf(t, again).Kept != "first" {
		t.Fatalf("second start replaced the run: %+v", again)
	}
	done := wait(t, eng, "order-7")
	if elapsed := time.Since(before); elapsed > 500*time.Millisecond {
		t.Fatalf("run finished after %v, want it enqueued without waiting for the maintenance tick", elapsed)
	}
	if s := stateOf(t, done); done.Status != engine.StatusDone || s.Kept != "first" || strings.Join(s.Log, ",") != "s0" {
		t.Fatalf("run = %q %+v", done.Status, s)
	}
}

func TestConditionalPark(t *testing.T) {
	wf := func() engine.Workflow {
		return engine.Workflow{
			Name: "wf",
			Steps: []engine.Step{
				appendStep("s0"),
				{
					Name: "gate", Input: engine.HookType[decisionInput]("decision-v1", nil),
					Run: engine.Typed(func(ctx context.Context, run *engine.Run, s *tState) error {
						if s.Amount >= 500 {
							if _, err := engine.HookInput[decisionInput](ctx, run, "review", ""); err != nil {
								return err
							}
						}
						s.Decision = ""
						s.Log = append(s.Log, "gate")
						return nil
					}),
				},
			},
		}
	}

	t.Run("low amount does not park", func(t *testing.T) {
		eng, _ := newTestEngine(t, wf())
		id, err := eng.Start(context.Background(), "wf", tState{Amount: 100})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		run := wait(t, eng, id)
		if run.Status != engine.StatusDone {
			t.Fatalf("status = %q, want done", run.Status)
		}
		if s := stateOf(t, run); !slices.Equal(s.Log, []string{"s0", "gate"}) {
			t.Fatalf("log = %v, want [s0 gate]", s.Log)
		}
	})

	t.Run("high amount parks until Resume accepts a decision", func(t *testing.T) {
		eng, _ := newTestEngine(t, wf())
		id, err := eng.Start(context.Background(), "wf", tState{Amount: 900})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		run := wait(t, eng, id, engine.StatusParked, engine.StatusDone, engine.StatusFailed)
		if run.Status != engine.StatusParked {
			t.Fatalf("status = %q, want parked", run.Status)
		}
		if name := eng.StepName(run); name != "gate" {
			t.Fatalf("parked at %q, want gate", name)
		}
		if s := stateOf(t, run); !slices.Equal(s.Log, []string{"s0"}) {
			t.Fatalf("log = %v, want [s0] — gate parked without proceeding", s.Log)
		}

		if _, err := eng.Resume(context.Background(), id, engine.Submission{}); err == nil {
			t.Fatal("empty submission accepted")
		}

		patch := struct {
			Decision string `json:"decision"`
		}{"approve"}
		if err := resumeTest(t, eng, id, patch); err != nil {
			t.Fatalf("Resume: %v", err)
		}
		run = wait(t, eng, id)
		if run.Status != engine.StatusDone {
			t.Fatalf("status = %q, want done", run.Status)
		}
		s := stateOf(t, run)
		if s.Decision != "" {
			t.Fatalf("decision = %q, want consumed (empty)", s.Decision)
		}
		if !slices.Equal(s.Log, []string{"s0", "gate"}) {
			t.Fatalf("log = %v, want [s0 gate]", s.Log)
		}
	})
}

func TestErrParkKeepsState(t *testing.T) {
	eng, _ := newTestEngine(t, engine.Workflow{
		Name: "wf",
		Steps: []engine.Step{{
			Name: "needs-input", Input: engine.HookType[decisionInput]("decision-v1", nil),
			Run: engine.Typed(func(ctx context.Context, run *engine.Run, s *tState) error {
				s.Kept = "kept"
				if _, err := engine.HookInput[decisionInput](ctx, run, "decision", ""); err != nil {
					return err
				}
				s.Log = append(s.Log, "went")
				return nil
			}),
		}},
	})
	id, err := eng.Start(context.Background(), "wf", nil)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	run := wait(t, eng, id, engine.StatusParked, engine.StatusDone, engine.StatusFailed)
	if run.Status != engine.StatusParked {
		t.Fatalf("status = %q, want parked", run.Status)
	}
	if s := stateOf(t, run); s.Kept != "kept" {
		t.Fatalf("kept = %q — a parking step's state changes must persist", s.Kept)
	}
	patch := struct {
		Decision string `json:"decision"`
	}{"go"}
	if err := resumeTest(t, eng, id, patch); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	run = wait(t, eng, id)
	if run.Status != engine.StatusDone {
		t.Fatalf("status = %q, want done", run.Status)
	}
	s := stateOf(t, run)
	if s.Kept != "kept" || !slices.Equal(s.Log, []string{"went"}) {
		t.Fatalf("state = %+v, want kept + [went]", s)
	}
}

func TestTerminalError(t *testing.T) {
	var attempts atomic.Int32
	eng, _ := newTestEngine(t, engine.Workflow{
		Name: "wf",
		Steps: []engine.Step{{
			Name: "reject",
			Run: func(ctx context.Context, run *engine.Run) error {
				attempts.Add(1)
				return fmt.Errorf("%w: bad input", engine.ErrTerminal)
			},
		}},
	})
	id, err := eng.Start(context.Background(), "wf", nil)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	run := wait(t, eng, id)
	if run.Status != engine.StatusFailed {
		t.Fatalf("status = %q, want failed", run.Status)
	}
	if !strings.Contains(run.Error, "bad input") {
		t.Fatalf("error = %q, want it to carry the step's message", run.Error)
	}
	if n := attempts.Load(); n != 1 {
		t.Fatalf("attempts = %d — a terminal error must not consume the retry budget", n)
	}
}

func TestRetryBudget(t *testing.T) {
	var attempts atomic.Int32
	eng, _ := newTestEngine(t, engine.Workflow{
		Name: "wf",
		Steps: []engine.Step{{
			Name: "flaky",
			Run: engine.Typed(func(ctx context.Context, run *engine.Run, s *tState) error {
				if attempts.Add(1) <= 4 {
					return errors.New("flaky")
				}
				s.Log = append(s.Log, "ok")
				return nil
			}),
		}},
	})
	id, err := eng.Start(context.Background(), "wf", nil)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	run := wait(t, eng, id)
	if run.Status != engine.StatusFailed {
		t.Fatalf("status = %q, want failed", run.Status)
	}
	if n := attempts.Load(); n != 3 {
		t.Fatalf("attempts = %d, want 3 (MaxDeliver)", n)
	}

	if err := eng.Retry(context.Background(), id); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	run = wait(t, eng, id)
	if run.Status != engine.StatusDone {
		t.Fatalf("status after Retry = %q, want done (err %q)", run.Status, run.Error)
	}
	if n := attempts.Load(); n != 5 {
		t.Fatalf("attempts = %d, want 5", n)
	}
}

func TestStaleReplay(t *testing.T) {
	eng, js := newTestEngine(t, engine.Workflow{
		Name:  "wf",
		Steps: []engine.Step{appendStep("s0"), appendStep("s1")},
	})
	ctx := context.Background()
	id, err := eng.Start(ctx, "wf", nil)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	wait(t, eng, id)

	data, _ := json.Marshal(engine.StepMsg{RunID: id, Step: 0})
	if _, err := js.Publish(ctx, "wf.step."+id, data); err != nil {
		t.Fatalf("publishing stale message: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	run, err := eng.GetRun(ctx, id)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.Status != engine.StatusDone {
		t.Fatalf("status = %q, want done — replay must not regress the run", run.Status)
	}
	if s := stateOf(t, run); !slices.Equal(s.Log, []string{"s0", "s1"}) {
		t.Fatalf("log = %v, want [s0 s1] — replay must not re-execute steps", s.Log)
	}
}

func TestCrashRecovery(t *testing.T) {
	eng, js := newTestEngine(t, engine.Workflow{
		Name: "wf",
		Steps: []engine.Step{
			appendStep("s0"),
			{Name: "s1", Run: engine.Typed(func(ctx context.Context, run *engine.Run, s *tState) error {
				if s.Decision != "approve" {
					return fmt.Errorf("%w: awaiting ack", engine.ErrPark)
				}
				s.Decision = ""
				s.Log = append(s.Log, "s1")
				return nil
			})},
		},
	})
	ctx := context.Background()
	id, err := eng.Start(ctx, "wf", nil)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	run := wait(t, eng, id, engine.StatusParked, engine.StatusDone, engine.StatusFailed)
	if run.Status != engine.StatusParked {
		t.Fatalf("status = %q, want parked", run.Status)
	}

	kv, err := js.KeyValue(ctx, "wf_runs")
	if err != nil {
		t.Fatalf("opening wf_runs: %v", err)
	}
	entry, err := kv.Get(ctx, id)
	if err != nil {
		t.Fatalf("reading run: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(entry.Value(), &m); err != nil {
		t.Fatalf("decoding run: %v", err)
	}
	m["status"] = string(engine.StatusRunning)
	m["execution"] = "recovered-wakeup"
	m["state"].(map[string]any)["decision"] = "approve"
	data, _ := json.Marshal(m)
	if _, err := kv.Put(ctx, id, data); err != nil {
		t.Fatalf("writing run: %v", err)
	}

	stale, _ := json.Marshal(engine.StepMsg{RunID: id, Step: 0})
	if _, err := js.Publish(ctx, "wf.step."+id, stale); err != nil {
		t.Fatalf("publishing stale message: %v", err)
	}

	run = wait(t, eng, id)
	if run.Status != engine.StatusDone {
		t.Fatalf("status = %q, want done — recovery must finish the run", run.Status)
	}
	if s := stateOf(t, run); !slices.Equal(s.Log, []string{"s0", "s1"}) {
		t.Fatalf("log = %v, want [s0 s1]", s.Log)
	}
}

func TestWaitTerminal(t *testing.T) {
	eng, _ := newTestEngine(t, engine.Workflow{
		Name:  "wf",
		Steps: []engine.Step{appendStep("s0")},
	})
	id, err := eng.Start(context.Background(), "wf", nil)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	wait(t, eng, id)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	run, err := eng.Wait(ctx, id)
	if err != nil || run.Status != engine.StatusDone {
		t.Fatalf("Wait on done run = (%v, %v), want immediate done", run, err)
	}

	if _, err := eng.Wait(ctx, "no-such-run"); err == nil {
		t.Fatal("Wait on unknown run: want an error")
	}
}

type decisionInput struct {
	Decision string `json:"decision"`
}

func resumeTest(t *testing.T, e *engine.Engine, id string, input any) error {
	t.Helper()
	r, err := e.GetRun(context.Background(), id)
	if err != nil {
		return err
	}
	data, _ := json.Marshal(input)
	_, err = e.Resume(context.Background(), id, engine.Submission{Token: r.HookWaits[len(r.HookWaits)-1].Token, SubmissionID: "test", Payload: data})
	return err
}

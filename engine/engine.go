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
	"strings"
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
	// The first delay overrides AckWait and is the default Step timeout. A
	// running Step heartbeats, so only crashed executors wait it out. Empty
	// defaults to 2m, 2m, 3m, 4m.
	BackOff []time.Duration
	// Must be at least len(BackOff)+1. Zero defaults to 5.
	MaxDeliver int
	// Zero defaults to 64 KiB.
	HookPayloadLimit int
	// Includes permanent token reservations. Zero defaults to 64 MiB.
	HookMaxBytes int64
	// Rejects new work when full. Zero defaults to 64 MiB.
	StepMaxBytes int64
	// Steps executing at once in this process. Zero defaults to 1.
	Concurrency int
	// Where the application serves Webhooks, such as https://app.example/hooks;
	// Run.Webhook appends the token. Empty disables Run.Webhook.
	WebhookURL string
	// Served by Webhooks next to Webhook tokens. See Trigger.
	Triggers []Trigger
}

// Wrap with %w to fail immediately without using the remaining retry budget.
var ErrTerminal = errors.New("terminal")

// ErrUnknownWorkflow is returned by Start, StartWithID, and Schedule when no
// registered workflow has the given name. Callers that must settle such a
// start rather than retry it match this error.
var ErrUnknownWorkflow = errors.New("unknown workflow")

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
	// Bounds one attempt; exceeding it counts as a failed attempt. Zero
	// defaults to the first BackOff delay.
	Timeout time.Duration
}

type Workflow struct {
	Name  string
	Steps []Step
	// Optional; nil produces an empty result.
	Result func(*Run) string
}

type Run struct {
	ID              string          `json:"id"`
	Workflow        string          `json:"workflow"`
	Status          Status          `json:"status"`
	Step            int             `json:"step"`
	Error           string          `json:"error,omitempty"`
	StartAt         *time.Time      `json:"start_at,omitempty"`
	ExpiresAt       *time.Time      `json:"expires_at,omitempty"`
	StartGeneration uint64          `json:"start_generation,omitempty"`
	State           json.RawMessage `json:"state,omitempty"`

	// The KV timestamp, populated on read rather than stored in the Run JSON.
	Updated   time.Time `json:"-"`
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
	webhookURL       string
	triggers         map[string]Trigger
	triggerLimit     int
	js               jetstream.JetStream
	kv               jetstream.KeyValue
	cons             jetstream.Consumer
	schedules        jetstream.Stream
	starts           jetstream.Consumer
	workflows        map[string]Workflow
	maxDeliver       int
	backOff          []time.Duration
	concurrency      int
	stepTimeout      time.Duration
	heartbeat        time.Duration
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
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 1
	}
	if cfg.Concurrency < 0 {
		return nil, errors.New("Concurrency must not be negative")
	}
	for _, wf := range workflows {
		for _, step := range wf.Steps {
			if step.Timeout < 0 {
				return nil, fmt.Errorf("workflow %q step %q has a negative timeout", wf.Name, step.Name)
			}
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

	e := &Engine{
		names: names, js: js, kv: kv, cons: cons, workflows: workflows, maxDeliver: cfg.MaxDeliver, log: log,
		backOff:     cfg.BackOff,
		concurrency: cfg.Concurrency,
		webhookURL:  strings.TrimSuffix(cfg.WebhookURL, "/"),
		stepTimeout: cfg.BackOff[0],
		// The server resets the ack timer to the shortest delay at worst.
		heartbeat: slices.Min(cfg.BackOff) / 3,
	}
	if err := e.initHooks(ctx, cfg); err != nil {
		return nil, err
	}
	if err := e.initTriggers(ctx, cfg.Triggers, nc.MaxPayload()); err != nil {
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

// StartWithID starts wf under the caller's id and returns the run. When a run
// with that id already exists, nothing starts and the existing run comes back
// unchanged, so a caller that may run twice starts the workflow once. A caller
// that needs the run to be its own compares the Workflow and State it gets.
func (e *Engine) StartWithID(ctx context.Context, id, wf string, initial any) (*Run, error) {
	if id == "" {
		return nil, errors.New("run id must not be empty")
	}
	run, err := e.newRun(id, wf, initial, nil)
	if err != nil {
		return nil, err
	}
	if err := e.putRun(ctx, run); err != nil {
		// Read back after any error, including an ambiguous acknowledgement timeout.
		existing, getErr := e.GetRun(ctx, id)
		if getErr != nil {
			return nil, fmt.Errorf("start run %s: %w", id, errors.Join(err, getErr))
		}
		if existing.Execution != run.Execution {
			return existing, nil
		}
		run = existing
	}
	return run, e.enqueue(ctx, run, 0)
}

func (e *Engine) createRun(ctx context.Context, wf string, initial any, at *time.Time) (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	run, err := e.newRun(id.String(), wf, initial, at)
	if err != nil {
		return "", err
	}
	if err := e.putRun(ctx, run); err != nil {
		return "", err
	}
	if at != nil {
		if err := e.publishSchedule(ctx, run); err != nil {
			e.log.Warn("schedule publication deferred", "run", run.ID, "err", err)
		}
		return run.ID, nil
	}
	return run.ID, e.enqueue(ctx, run, 0)
}

func (e *Engine) newRun(id, wf string, initial any, at *time.Time) (*Run, error) {
	if _, ok := e.workflows[wf]; !ok {
		return nil, fmt.Errorf("%w %q", ErrUnknownWorkflow, wf)
	}
	run := &Run{ID: id, Workflow: wf, Status: StatusRunning, Execution: uuid.NewString()}
	if at != nil {
		run.Status = StatusScheduled
		run.StartAt = at
	}
	if initial != nil {
		data, err := json.Marshal(initial)
		if err != nil {
			return nil, err
		}
		run.State = data
	}
	return run, nil
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
	stepsClosed := make(chan struct{})
	go func() {
		defer close(stepsClosed)
		e.executeSteps(ctx, &callbacks)
	}()
	startsClosed := sc.Closed()
	runWatch, err := e.kv.WatchAll(ctx)
	if err != nil {
		return unlessCancelled(ctx, err)
	}
	defer runWatch.Stop()
	hookWatch, err := e.hooks.WatchAll(ctx)
	if err != nil {
		return unlessCancelled(ctx, err)
	}
	defer hookWatch.Stop()
	e.sweepOrphanedHooks(ctx)
	runs := map[string]*watchedRun{}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-startsClosed:
			return errors.New("schedule consumer stopped unexpectedly")
		case <-stepsClosed:
			return unlessCancelled(ctx, errors.New("step consumer stopped unexpectedly"))
		case entry, ok := <-runWatch.Updates():
			if !ok {
				return unlessCancelled(ctx, errors.New("run watch stopped unexpectedly"))
			}
			// A nil entry ends the stored Runs.
			if entry == nil {
				e.maintainRuns(ctx, runs)
				continue
			}
			e.observeRun(runs, entry)
		case entry, ok := <-hookWatch.Updates():
			if !ok {
				return unlessCancelled(ctx, errors.New("hook watch stopped unexpectedly"))
			}
			if entry != nil {
				observeHook(runs, entry)
			}
		case <-ticker.C:
			e.maintainRuns(ctx, runs)
		}
	}
}

func unlessCancelled(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return nil
	}
	return err
}

// executeSteps pulls a Step only when a slot is free. A prefetched delivery
// waiting for a slot would outlive its ack wait and run twice.
func (e *Engine) executeSteps(ctx context.Context, callbacks *sync.RWMutex) {
	slots := make(chan struct{}, e.concurrency)
	for {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		msg, err := e.nextStep(ctx)
		if msg == nil {
			<-slots
			if ctx.Err() != nil || errors.Is(err, nats.ErrConnectionClosed) ||
				errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, jetstream.ErrConsumerNotFound) {
				return
			}
			if err != nil {
				e.log.Warn("fetching step", "err", err)
				select {
				case <-time.After(time.Second):
				case <-ctx.Done():
					return
				}
			}
			continue
		}
		callbacks.RLock()
		go func() {
			defer func() { <-slots }()
			defer callbacks.RUnlock()
			if ctx.Err() == nil {
				e.handle(ctx, msg)
			}
		}()
	}
}

func (e *Engine) nextStep(ctx context.Context) (jetstream.Msg, error) {
	pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	batch, err := e.cons.Fetch(1, jetstream.FetchContext(pollCtx))
	if err != nil {
		return nil, err
	}
	for msg := range batch.Messages() {
		return msg, nil
	}
	if err := batch.Error(); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return nil, err
	}
	return nil, nil
}

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
	if err := e.runStep(ctx, msg, step, run); err != nil {
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

func (e *Engine) runStep(ctx context.Context, msg jetstream.Msg, step Step, run *Run) error {
	stepCtx, cancel := context.WithTimeout(ctx, cmp.Or(step.Timeout, e.stepTimeout))
	defer cancel()
	done := make(chan struct{})
	defer close(done)
	go func() {
		ticker := time.NewTicker(e.heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if err := msg.InProgress(); err != nil {
					e.log.Warn("step heartbeat", "run", run.ID, "step", run.Step, "err", err)
				}
			}
		}
	}()
	return step.Run(stepCtx, run)
}

func (e *Engine) fail(ctx context.Context, msg jetstream.Msg, run *Run, err error) {
	md, mdErr := msg.Metadata()
	if !errors.Is(err, ErrTerminal) && mdErr == nil && md.NumDelivered < uint64(e.maxDeliver) {
		e.log.Warn("step failed, will retry", "run", run.ID, "step", run.Step, "attempt", md.NumDelivered, "err", err)
		msg.NakWithDelay(e.backOff[min(int(md.NumDelivered)-1, len(e.backOff)-1)])
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
// current value first, so an already-terminal Run returns immediately. When ctx
// ends first, Wait returns the last observed Run and an error wrapping ctx.Err().
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
			return last, fmt.Errorf("timed out waiting for run: %w", ctx.Err())
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

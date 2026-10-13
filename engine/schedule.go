package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var ErrStartTime = errors.New("start_at must be in the future")
var ErrConflict = errors.New("run changed or is no longer scheduled")

type startMessage struct {
	RunID      string `json:"run_id"`
	Generation uint64 `json:"generation"`
}

func (e *Engine) initSchedules(ctx context.Context) error {
	stream, err := e.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: e.names.schedules, Subjects: []string{e.names.subject("schedule.>"), e.names.subject("start.>")},
		Storage: jetstream.FileStorage, Retention: jetstream.WorkQueuePolicy,
		AllowMsgSchedules: true,
	})
	if err != nil {
		return fmt.Errorf("creating schedules: %w", err)
	}
	e.schedules = stream
	e.starts, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: "starter", FilterSubject: e.names.subject("start.>"), AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: 30 * time.Second, MaxDeliver: -1,
	})
	return err
}

// Scheduled Runs cannot start until a worker is serving.
func (e *Engine) Schedule(ctx context.Context, workflow string, initial any, at time.Time) (string, error) {
	if !at.After(time.Now()) {
		return "", ErrStartTime
	}
	at = at.UTC()
	return e.createRun(ctx, workflow, initial, &at)
}

func (e *Engine) Cancel(ctx context.Context, id string) error {
	run, err := e.GetRun(ctx, id)
	if err != nil {
		return err
	}
	if run.Status != StatusScheduled {
		return ErrConflict
	}
	old := e.scheduleSubject(run)
	run.Status = StatusCancelled
	if err := e.putRun(ctx, run); err != nil {
		return fmt.Errorf("%w: %v", ErrConflict, err)
	}
	e.removeSchedule(ctx, old)
	return nil
}

func (e *Engine) Reschedule(ctx context.Context, id string, at time.Time) error {
	if !at.After(time.Now()) {
		return ErrStartTime
	}
	run, err := e.GetRun(ctx, id)
	if err != nil {
		return err
	}
	if run.Status != StatusScheduled {
		return ErrConflict
	}
	old := e.scheduleSubject(run)
	at = at.UTC()
	run.StartAt = &at
	if err := e.putRun(ctx, run); err != nil {
		return fmt.Errorf("%w: %v", ErrConflict, err)
	}
	e.removeSchedule(ctx, old)
	if err := e.publishSchedule(ctx, run); err != nil {
		e.log.Warn("schedule publication deferred", "run", id, "err", err)
	}
	return nil
}

func (e *Engine) scheduleSubject(run *Run) string {
	return e.names.subject(fmt.Sprintf("schedule.%s.%d", run.ID, run.rev))
}

func (e *Engine) removeSchedule(ctx context.Context, subject string) {
	if err := e.schedules.Purge(ctx, jetstream.WithPurgeSubject(subject)); err != nil {
		e.log.Warn("removing obsolete schedule", "subject", subject, "err", err)
	}
}

func (e *Engine) publishSchedule(ctx context.Context, run *Run) error {
	data, err := json.Marshal(startMessage{RunID: run.ID, Generation: run.rev})
	if err != nil {
		return err
	}
	subject := e.scheduleSubject(run)
	if !run.StartAt.After(time.Now()) {
		_, err = e.js.Publish(ctx, e.names.subject("start."+run.ID), data, jetstream.WithMsgID(subject))
		return err
	}
	if _, err := e.schedules.GetLastMsgForSubject(ctx, subject); err == nil {
		return nil
	} else if !errors.Is(err, jetstream.ErrMsgNotFound) {
		return err
	}
	msg := nats.NewMsg(subject)
	msg.Data = data
	// Preserve fractional seconds: WithScheduleAt currently formats whole seconds.
	msg.Header.Set("Nats-Schedule", "@at "+run.StartAt.Format(time.RFC3339Nano))
	msg.Header.Set("Nats-Schedule-Target", e.names.subject("start."+run.ID))
	_, err = e.js.PublishMsg(ctx, msg, jetstream.WithExpectLastSequencePerSubject(0))
	return err
}

func (e *Engine) handleStart(ctx context.Context, msg jetstream.Msg) {
	var start startMessage
	if json.Unmarshal(msg.Data(), &start) != nil {
		msg.Term()
		return
	}
	run, err := e.GetRun(ctx, start.RunID)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		msg.Ack()
		return
	}
	if err != nil {
		msg.NakWithDelay(time.Second)
		return
	}
	if run.Status == StatusScheduled && run.rev == start.Generation {
		if run.StartAt.After(time.Now()) {
			msg.NakWithDelay(time.Until(*run.StartAt))
			return
		}
		run.Status = StatusRunning
		run.StartGeneration = start.Generation
		if e.putRun(ctx, run) != nil {
			msg.NakWithDelay(time.Second)
			return
		}
	}
	if run.Status == StatusRunning && run.Step == 0 && run.StartGeneration == start.Generation {
		if e.enqueue(ctx, run, 0) != nil {
			msg.NakWithDelay(time.Second)
			return
		}
	}
	msg.Ack()
}

type upkeep int

const (
	unsettled upkeep = iota
	settled
	hookCleanupPending
)

type watchedRun struct {
	stored *Run
	upkeep upkeep
}

func (w *watchedRun) reconcileDue(now time.Time) bool {
	startDue := w.stored.Status == StatusScheduled && !w.stored.StartAt.After(now)
	return w.upkeep == unsettled || startDue
}

func (e *Engine) observeRun(runs map[string]*watchedRun, entry jetstream.KeyValueEntry) {
	if entry.Operation() != jetstream.KeyValuePut {
		if w := runs[entry.Key()]; w != nil {
			w.upkeep = hookCleanupPending
		}
		return
	}
	run, err := runFromEntry(entry)
	if err != nil {
		e.log.Warn("skipping unreadable run", "key", entry.Key(), "err", err)
		delete(runs, entry.Key())
		return
	}
	runs[entry.Key()] = &watchedRun{stored: run}
}

func observeHook(runs map[string]*watchedRun, entry jetstream.KeyValueEntry) {
	var h Hook
	if entry.Operation() != jetstream.KeyValuePut || json.Unmarshal(entry.Value(), &h) != nil {
		return
	}
	if w := runs[h.RunID]; w != nil && w.upkeep == settled {
		w.upkeep = unsettled
	}
}

func (e *Engine) maintainRuns(ctx context.Context, runs map[string]*watchedRun) {
	now := time.Now()
	for id, w := range runs {
		if w.upkeep != hookCleanupPending && w.stored.Status.Terminal() {
			expiry := w.stored.ExpiresAt
			if expiry == nil {
				legacy := w.stored.Updated.Add(7 * 24 * time.Hour)
				expiry = &legacy
			}
			if !expiry.After(now) {
				if err := e.kv.Purge(ctx, w.stored.ID, jetstream.LastRevision(w.stored.rev)); err != nil {
					e.log.Warn("expiring run", "run", w.stored.ID, "err", err)
					continue
				}
				w.upkeep = hookCleanupPending
			}
		}
		switch {
		case w.upkeep == hookCleanupPending:
			if e.stripHooksOfRemovedRun(ctx, w.stored) == nil {
				delete(runs, id)
			}
		case w.reconcileDue(now):
			w.upkeep = e.reconcileRun(ctx, *w.stored)
			if w.stored.Status == StatusScheduled {
				if err := e.publishSchedule(ctx, w.stored); err != nil {
					e.log.Warn("repairing schedule", "run", w.stored.ID, "err", err)
					w.upkeep = unsettled
				}
			}
		}
	}
	// Info mutates the client's cached stream metadata. Use a separate handle
	// so concurrent schedule lookups do not race that mutation in nats.go.
	snapshot, err := e.js.Stream(ctx, e.names.schedules)
	if err != nil {
		return
	}
	info, err := snapshot.Info(ctx, jetstream.WithSubjectFilter(e.names.subject("schedule.>")))
	if err != nil {
		return
	}
	for subject := range info.State.Subjects {
		parts := strings.Split(subject, ".")
		if len(parts) != 4 {
			continue
		}
		if w := runs[parts[2]]; w != nil && w.upkeep != hookCleanupPending && w.stored.Status == StatusScheduled && e.scheduleSubject(w.stored) == subject {
			continue
		}
		run, err := e.GetRun(ctx, parts[2])
		if errors.Is(err, jetstream.ErrKeyNotFound) || (err == nil && (run.Status != StatusScheduled || e.scheduleSubject(run) != subject)) {
			e.removeSchedule(ctx, subject)
		}
	}
}

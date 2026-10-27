package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
)

// HookTrigger marks a token reservation held by a Trigger, not by a Run.
const HookTrigger HookStatus = "trigger"

// Room left in a NATS message for the Run fields around a trigger body.
const runEnvelopeBytes = 4 << 10

const triggerStartTimeout = 5 * time.Second

// Trigger is a stable token, chosen by the application, that starts a new Run
// of Workflow for every verified POST to <Config.WebhookURL>/<Token>. The
// request body becomes the Run's initial State.
type Trigger struct {
	// 1-256 ASCII letters, digits, underscores or hyphens. It shares the token
	// space of Hooks and is never given to anything else, even after the
	// Trigger is removed.
	Token    string
	Workflow string
	// Created by WebhookType for the workflow's State type. Its verify,
	// decoding and accept checks run before a Run starts.
	Input *InputContract
	// Names the delivery, so a repeated key is a duplicate and starts no Run.
	// Nil uses the SHA-256 of the body, which treats any changed byte as a new
	// delivery.
	Key func(body []byte) (string, error)
}

func validTriggerToken(s string) bool {
	if s == "" || len(s) > 256 {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}

func (e *Engine) initTriggers(ctx context.Context, triggers []Trigger, maxPayload int64) error {
	e.triggerLimit = min(e.hookPayloadLimit, int(maxPayload)-runEnvelopeBytes)
	e.triggers = make(map[string]Trigger, len(triggers))
	for _, t := range triggers {
		if !validTriggerToken(t.Token) {
			return fmt.Errorf("trigger token %q: use 1-256 ASCII letters, digits, underscores or hyphens", t.Token)
		}
		if t.Input == nil || t.Input.verify == nil {
			return fmt.Errorf("trigger %q requires an Input created by WebhookType", t.Token)
		}
		if _, ok := e.workflows[t.Workflow]; !ok {
			return fmt.Errorf("trigger %q: %w %q", t.Token, ErrUnknownWorkflow, t.Workflow)
		}
		if _, dup := e.triggers[t.Token]; dup {
			return fmt.Errorf("duplicate trigger token %q", t.Token)
		}
		e.triggers[t.Token] = t
	}
	// Reservations are permanent, so none is made until every Trigger is valid.
	for _, t := range triggers {
		if err := e.reserveTrigger(ctx, t); err != nil {
			return err
		}
	}
	return nil
}

// reserveTrigger claims the token so RegisterHook can never give it to a Run,
// and refuses a token that a Hook or another workflow's Trigger holds.
func (e *Engine) reserveTrigger(ctx context.Context, t Trigger) error {
	h := &Hook{Name: "trigger", Token: t.Token, Contract: "trigger:" + t.Workflow, Status: HookTrigger}
	err := e.saveHook(ctx, h)
	if !errors.Is(err, jetstream.ErrKeyExists) {
		if err != nil {
			return fmt.Errorf("reserving trigger %q: %w", t.Token, err)
		}
		return nil
	}
	held, err := e.GetHook(ctx, t.Token)
	if err != nil {
		return err
	}
	if held.Status != HookTrigger {
		return fmt.Errorf("trigger token %q is already a hook token of run %q", t.Token, held.RunID)
	}
	if held.Contract != h.Contract {
		return fmt.Errorf("trigger token %q is reserved for %s, not %s", t.Token, held.Contract, h.Contract)
	}
	return nil
}

func triggerRunID(token, key string) string {
	space := uuid.NewSHA1(uuid.NameSpaceURL, []byte("nats-workflows/trigger/"+token))
	return uuid.NewSHA1(space, []byte(key)).String()
}

func (e *Engine) serveTrigger(w http.ResponseWriter, r *http.Request, t Trigger, body []byte) {
	if len(body) > e.triggerLimit {
		http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
		return
	}
	if err := t.Input.verify(Request{Header: r.Header, Body: body}); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if err := t.Input.validate(body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if t.Input.accept != nil && !t.Input.accept(body) {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	sum := sha256.Sum256(body)
	key := hex.EncodeToString(sum[:])
	if t.Key != nil {
		var err error
		if key, err = t.Key(body); err != nil || key == "" {
			http.Error(w, "no delivery key", http.StatusBadRequest)
			return
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), triggerStartTimeout)
	defer cancel()
	id := triggerRunID(t.Token, key)
	run, err := e.StartWithID(ctx, id, t.Workflow, json.RawMessage(body))
	if run == nil {
		e.log.Error("trigger start", "run", id, "error", err)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	if err != nil {
		// The Run is stored, and Serve publishes its first Step.
		e.log.Warn("trigger enqueue deferred", "run", id, "error", err)
	}
	w.WriteHeader(http.StatusAccepted)
}

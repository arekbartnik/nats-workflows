package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/arekbartnik/nats-workflows/engine"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestScheduledCLI(t *testing.T) {
	ns, err := server.NewServer(&server.Options{DontListen: true, JetStream: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS not ready")
	}
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	nc, err := nats.Connect("", nats.InProcessServer(ns))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	ctx := context.Background()
	var wfs []engine.Workflow
	for _, name := range []string{"swapi", "swapi-choose", "ask", "order"} {
		wfs = append(wfs, engine.Workflow{Name: name, Steps: []engine.Step{{Name: "done", Run: func(context.Context, *engine.Run) error { return nil }}}})
	}
	eng, err := engine.New(ctx, nc, slog.New(slog.NewTextHandler(io.Discard, nil)), engine.Config{}, wfs...)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().Add(time.Hour).Format(time.RFC3339Nano)
	for _, tc := range []struct {
		cmd  string
		args []string
		wf   string
	}{
		{"random", []string{"--at", at}, "swapi"},
		{"choose", []string{"--at=" + at}, "swapi-choose"},
		{"ask", []string{"question?", "--at", at}, "ask"},
		{"order", []string{"750", "--at", at}, "order"},
	} {
		handled, err := scheduledCommand(ctx, eng, tc.cmd, tc.args)
		if !handled || err != nil {
			t.Fatalf("%s: %v", tc.cmd, err)
		}
		runs, err := eng.ListRuns(ctx)
		if err != nil {
			t.Fatal(err)
		}
		run := runs[0]
		if run.Workflow != tc.wf || run.Status != engine.StatusScheduled {
			t.Fatalf("%+v", run)
		}
		if _, err := scheduledCommand(ctx, eng, "reschedule", []string{run.ID, at}); err != nil {
			t.Fatal(err)
		}
		if _, err := scheduledCommand(ctx, eng, "cancel", []string{run.ID}); err != nil {
			t.Fatal(err)
		}
	}
	if handled, err := scheduledCommand(ctx, eng, "serve", nil); handled || err != nil {
		t.Fatal("intercepted serve")
	}
	if _, err := scheduledCommand(ctx, eng, "order", []string{"--at", "2027-01-01T00:00:00"}); err == nil {
		t.Fatal("accepted time without timezone")
	}
}

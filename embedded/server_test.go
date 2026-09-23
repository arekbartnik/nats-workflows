package embedded_test

import (
	"context"
	"testing"

	"github.com/arekbartnik/nats-workflows/embedded"
	"github.com/arekbartnik/nats-workflows/engine"
	"github.com/nats-io/nats.go"
)

func TestEmbeddedStoreSurvivesRestart(t *testing.T) {
	cfg := embedded.Config{StoreDir: t.TempDir()}
	wf := engine.Workflow{Name: "wf", Steps: []engine.Step{{Name: "done", Run: func(context.Context, *engine.Run) error { return nil }}}}
	srv, err := embedded.Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)
	if srv.ClientURL() != "" {
		t.Fatalf("enabled TCP by default: %s", srv.ClientURL())
	}
	ctx := context.Background()
	eng, err := engine.New(ctx, srv.Conn(), nil, engine.Config{Namespace: "app"}, wf)
	if err != nil {
		t.Fatal(err)
	}
	id, err := eng.Start(ctx, "wf", map[string]string{"value": "persisted"})
	if err != nil {
		t.Fatal(err)
	}
	srv.Close()
	srv.Close()
	restarted, err := embedded.Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	eng, err = engine.New(ctx, restarted.Conn(), nil, engine.Config{Namespace: "app"}, wf)
	if err != nil {
		t.Fatal(err)
	}
	run, err := eng.GetRun(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	state, err := engine.StateOf[map[string]string](run)
	if err != nil || state["value"] != "persisted" {
		t.Fatalf("state after restart: %v, %v", state, err)
	}
}

func TestEmbeddedOptionalTCP(t *testing.T) {
	srv, err := embedded.Start(embedded.Config{StoreDir: t.TempDir(), ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if srv.ClientURL() == "" {
		t.Fatal("missing TCP address")
	}
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestEmbeddedRejectsInvalidConfiguration(t *testing.T) {
	for _, cfg := range []embedded.Config{
		{},
		{StoreDir: t.TempDir(), ListenAddr: "localhost"},
		{StoreDir: t.TempDir(), ListenAddr: "localhost:-1"},
		{StoreDir: t.TempDir(), ListenAddr: "localhost:65536"},
	} {
		if srv, err := embedded.Start(cfg); err == nil {
			srv.Close()
			t.Fatalf("accepted invalid configuration: %+v", cfg)
		}
	}
}

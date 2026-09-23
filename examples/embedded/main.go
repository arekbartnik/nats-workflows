package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/arekbartnik/nats-workflows/embedded"
	"github.com/arekbartnik/nats-workflows/engine"
)

type greetingState struct {
	Name     string `json:"name"`
	Greeting string `json:"greeting"`
}

func main() {
	dir := flag.String("data", "data/example", "exclusive JetStream storage directory")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := run(ctx, *dir, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, storeDir string, out io.Writer) (err error) {
	srv, err := embedded.Start(embedded.Config{StoreDir: storeDir})
	if err != nil {
		return err
	}
	defer srv.Close()

	wf := engine.Workflow{
		Name: "greet",
		Steps: []engine.Step{{Name: "hello", Run: engine.Typed(
			func(ctx context.Context, r *engine.Run, s *greetingState) error {
				s.Greeting = "Hello, " + s.Name + "!"
				return nil
			},
		)}},
	}
	eng, err := engine.New(ctx, srv.Conn(), slog.Default(), engine.Config{Namespace: "example"}, wf)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	var workerErr error
	go func() {
		workerErr = eng.Serve(ctx)
		cancel() // Wake callers if the worker stops unexpectedly.
		close(done)
	}()
	defer func() {
		cancel()
		<-done // Wait for callbacks before closing NATS.
		err = errors.Join(err, workerErr)
	}()

	id, err := eng.Start(ctx, "greet", greetingState{Name: "Ada"})
	if err != nil {
		return err
	}
	result, err := eng.Wait(ctx, id, engine.StatusDone, engine.StatusFailed)
	if err != nil {
		return err
	}
	if result.Status == engine.StatusFailed {
		return fmt.Errorf("workflow failed: %s", result.Error)
	}
	state, err := engine.StateOf[greetingState](result)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, state.Greeting)
	return err
}

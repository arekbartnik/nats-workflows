//go:build unix

package embedded_test

import (
	"bytes"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/arekbartnik/nats-workflows/embedded"
)

// Use a subprocess because a NATS signal handler can exit the entire process.
func TestEmbeddedLeavesSignalsToHost(t *testing.T) {
	if os.Getenv("NATS_WORKFLOWS_SIGNAL_TEST") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestEmbeddedLeavesSignalsToHost$")
		cmd.Env = append(os.Environ(), "NATS_WORKFLOWS_SIGNAL_TEST=1")
		out, err := cmd.CombinedOutput()
		if err != nil || !bytes.Contains(out, []byte("host handled SIGTERM")) {
			t.Fatalf("embedded server intercepted host signal: %s, %v", out, err)
		}
		return
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	defer signal.Stop(signals)
	srv, err := embedded.Start(embedded.Config{StoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-signals:
	case <-time.After(5 * time.Second):
		t.Fatal("host did not receive SIGTERM")
	}
	// Give an accidentally installed NATS signal handler time to run as well.
	time.Sleep(100 * time.Millisecond)
	if err := srv.Conn().Flush(); err != nil {
		t.Fatalf("signal shut down the host's server: %v", err)
	}
	os.Stdout.WriteString("host handled SIGTERM\n")
}

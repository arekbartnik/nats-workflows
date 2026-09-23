package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/arekbartnik/nats-workflows/embedded"
	"github.com/nats-io/nats.go"
)

// Join the local server to avoid opening the same store in two processes.
const defaultNATSURL = "nats://127.0.0.1:4222"

// An explicit NATS_URL must fail rather than fall back to another store.
func startNATS(log *slog.Logger, listen bool) (*nats.Conn, func(), error) {
	url, explicit := os.Getenv("NATS_URL"), true
	if url == "" {
		url, explicit = defaultNATSURL, false
	}
	opts := []nats.Option{nats.Name("nats-workflows")}
	if !explicit {
		opts = append(opts, nats.Timeout(500*time.Millisecond))
	}
	nc, err := nats.Connect(url, opts...)
	if err == nil {
		log.Info("connected to NATS", "url", url)
		return nc, func() { nc.Close() }, nil
	}
	if explicit {
		return nil, nil, fmt.Errorf("connecting to NATS_URL %s: %w", url, err)
	}

	dir := os.Getenv("NATS_DATA")
	if dir == "" {
		dir = "data"
	}

	cfg := embedded.Config{StoreDir: dir}
	if listen {
		cfg.ListenAddr = "127.0.0.1:4222"
	}
	ns, err := embedded.Start(cfg)
	if err != nil {
		return nil, nil, err
	}

	log.Info("embedded NATS server running", "store_dir", dir, "listening", listen)
	return ns.Conn(), ns.Close, nil
}

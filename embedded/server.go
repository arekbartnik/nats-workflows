package embedded

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type Config struct {
	// StoreDir is required and must be used exclusively by this server.
	StoreDir string
	// Empty disables TCP. Port 0 chooses a free port.
	ListenAddr string
}

// Stop and wait for engine workers before Close. A Server must not be copied.
type Server struct {
	server *server.Server
	conn   *nats.Conn
	once   sync.Once
}

func Start(cfg Config) (*Server, error) {
	if cfg.StoreDir == "" {
		return nil, errors.New("embedded NATS requires StoreDir")
	}
	opts := &server.Options{
		ServerName: "nats-workflows-embedded",
		NoSigs:     true, // Only the host app may handle signals or exit the process.
		DontListen: cfg.ListenAddr == "",
		JetStream:  true,
		StoreDir:   cfg.StoreDir,
	}
	if cfg.ListenAddr != "" {
		host, port, err := net.SplitHostPort(cfg.ListenAddr)
		if err != nil {
			return nil, fmt.Errorf("invalid ListenAddr: %w", err)
		}
		n, err := strconv.Atoi(port)
		if err != nil || n < 0 || n > 65535 {
			return nil, fmt.Errorf("invalid ListenAddr port %q", port)
		}
		if n == 0 {
			n = server.RANDOM_PORT
		}
		opts.Host, opts.Port = host, n
	}
	ns, err := server.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("creating embedded NATS: %w", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		ns.Shutdown()
		ns.WaitForShutdown()
		return nil, errors.New("embedded NATS did not become ready within ten seconds")
	}
	nc, err := nats.Connect("", nats.InProcessServer(ns), nats.Name("nats-workflows"))
	if err != nil {
		ns.Shutdown()
		ns.WaitForShutdown()
		return nil, fmt.Errorf("connecting to embedded NATS: %w", err)
	}
	return &Server{server: ns, conn: nc}, nil
}

func (s *Server) Conn() *nats.Conn { return s.conn }

// Returns an empty string when TCP is disabled.
func (s *Server) ClientURL() string {
	if s.server.Addr() == nil {
		return ""
	}
	return s.server.ClientURL()
}

// Close is safe to repeat and leaves persisted data intact.
func (s *Server) Close() {
	s.once.Do(func() {
		s.conn.Close()
		s.server.Shutdown()
		s.server.WaitForShutdown()
	})
}

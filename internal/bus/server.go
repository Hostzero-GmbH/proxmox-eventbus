// Package bus runs an embedded nats-server that auto-clusters with peers
// discovered from /etc/pve/.members and exposes an in-process publisher.
package bus

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/pmxcfs"
	natsserver "github.com/nats-io/nats-server/v2/server"
	nats "github.com/nats-io/nats.go"
)

type Options struct {
	ServerName string

	ClientHost string
	ClientPort int

	ClusterHost string
	ClusterPort int

	TLS        *tls.Config // for the client listener
	ClusterTLS *tls.Config // for the cluster route listener

	StaticRoutes []string
	Reader       *pmxcfs.Reader

	Logger Logger
}

// Logger is a subset of slog used by the embedded NATS adapter and the route manager.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type Server struct {
	opts Options
	ns   *natsserver.Server
	conn *nats.Conn

	natsOpts *natsserver.Options // retained for ReloadOptions on route changes

	cancelRouteMgr context.CancelFunc
}

func New(o Options) *Server {
	return &Server{opts: o}
}

func (s *Server) Start(ctx context.Context) error {
	nopts := &natsserver.Options{
		ServerName: s.opts.ServerName,
		Host:       s.opts.ClientHost,
		Port:       s.opts.ClientPort,
		NoSigs:     true,
		HTTPPort:   -1,
		Cluster: natsserver.ClusterOpts{
			Name:      "proxmox-eventbus",
			Host:      s.opts.ClusterHost,
			Port:      s.opts.ClusterPort,
			TLSConfig: s.opts.ClusterTLS,
		},
		TLSConfig: s.opts.TLS,
	}
	if s.opts.TLS != nil {
		nopts.TLS = true
		nopts.AllowNonTLS = false
		nopts.TLSVerify = s.opts.TLS.ClientAuth == tls.RequireAndVerifyClientCert
		nopts.TLSTimeout = 5
	}
	if s.opts.ClusterTLS != nil {
		nopts.Cluster.TLSTimeout = 5
	}

	nopts.Routes = append(nopts.Routes, routesToURLs(s.opts.StaticRoutes)...)

	ns, err := natsserver.NewServer(nopts)
	if err != nil {
		return fmt.Errorf("nats-server: %w", err)
	}
	if s.opts.Logger != nil {
		ns.SetLoggerV2(&natsLogAdapter{l: s.opts.Logger}, false, false, false)
	}
	s.ns = ns
	s.natsOpts = nopts

	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		return errors.New("nats-server failed to become ready")
	}

	if err := s.connectInProc(); err != nil {
		return err
	}

	if s.opts.Reader != nil {
		rctx, cancel := context.WithCancel(ctx)
		s.cancelRouteMgr = cancel
		go s.runRouteManager(rctx)
	}
	return nil
}

func (s *Server) Stop() {
	if s.cancelRouteMgr != nil {
		s.cancelRouteMgr()
	}
	if s.conn != nil {
		s.conn.Close()
	}
	if s.ns != nil {
		s.ns.Shutdown()
	}
}

// PublishJSON publishes v as JSON on the given subject. Uses the embedded
// in-process connection so latency stays sub-millisecond and we never go
// through the network.
func (s *Server) PublishJSON(subject string, v any) error {
	if s.conn == nil {
		return errors.New("bus not started")
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return s.conn.Publish(subject, b)
}

// Conn returns the underlying core NATS connection for subscriptions and tests.
func (s *Server) Conn() *nats.Conn { return s.conn }

func (s *Server) connectInProc() error {
	nc, err := nats.Connect("",
		nats.InProcessServer(s.ns),
		nats.Name("proxmox-eventbus/local"),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		return fmt.Errorf("in-proc connect: %w", err)
	}
	s.conn = nc
	return nil
}

func routesToURLs(raw []string) []*url.URL {
	out := make([]*url.URL, 0, len(raw))
	for _, r := range raw {
		u, err := url.Parse(r)
		if err == nil {
			out = append(out, u)
		}
	}
	return out
}

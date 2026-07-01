// proxmox-eventbus runs an embedded NATS server on a PVE node and publishes
// CloudEvents for VM/LXC lifecycle and periodic state snapshots.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/bus"
	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/config"
	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/enrich"
	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/events"
	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/journal"
	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/pmxcfs"
	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/snapshot"
	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/tasks"
	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/tlsutil"
	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/version"
	nats "github.com/nats-io/nats.go"
)

type natsMsg = nats.Msg

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "issue-client-cert":
			os.Exit(runIssueClientCert(os.Args[2:]))
		case "tail":
			os.Exit(runTail(os.Args[2:]))
		case "version", "-v", "--version":
			fmt.Println(version.String())
			return
		case "help", "-h", "--help":
			printHelp()
			return
		}
	}
	if err := runDaemon(); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func printHelp() {
	fmt.Println(`proxmox-eventbus ` + version.String() + `

Usage:
  proxmox-eventbus                    run the daemon (reads /etc/proxmox-eventbus/config.yaml)
  proxmox-eventbus tail <subject>...  subscribe over local NATS and print events
  proxmox-eventbus issue-client-cert  mint a client cert signed by the PVE cluster CA
  proxmox-eventbus version            print version
  proxmox-eventbus help               print this help`)
}

func runDaemon() error {
	cfgPath := flag.String("config", config.DefaultPath, "path to config file")
	if err := flag.CommandLine.Parse(os.Args[1:]); err != nil {
		return err
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	slog.SetDefault(slog.New(journal.NewHandler(cfg.Logging.Format, cfg.Logging.Level)))
	log := slog.Default()
	log.Info("proxmox-eventbus starting", "version", version.String(), "config", *cfgPath)

	reader := pmxcfs.New(cfg.Node.PmxcfsRoot)

	clusterName := nonEmpty(strings.TrimSpace(cfg.Node.ClusterName), detectClusterName(reader, log))
	if clusterName == "" {
		clusterName = "standalone"
	}
	nodeName := cfg.Node.Name
	if nodeName == "" {
		if n, err := reader.LocalNodeName(); err == nil {
			nodeName = n
		}
	}
	if nodeName == "" {
		host, _ := os.Hostname()
		nodeName = host
	}
	log.Info("identity resolved", "cluster", clusterName, "node", nodeName)

	tlsLoader, err := tlsutil.NewLoader(
		cfg.NATS.TLS.CAFile,
		cfg.NATS.TLS.CertFile,
		cfg.NATS.TLS.KeyFile,
		cfg.NATS.TLS.VerifyClient,
	)
	if err != nil {
		return fmt.Errorf("tls: %w", err)
	}
	log.Info("cluster route TLS: chain verified against CA, hostname check disabled",
		"ca", cfg.NATS.TLS.CAFile, "cert", cfg.NATS.TLS.CertFile)

	clientAdv, clusterAdv := resolveAdvertise(cfg, reader, log)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	server := bus.New(bus.Options{
		ServerName:       nonEmpty(cfg.NATS.ServerName, nodeName),
		ClientHost:       cfg.NATS.Client.Host,
		ClientPort:       cfg.NATS.Client.Port,
		ClientAdvertise:  clientAdv,
		ClusterHost:      cfg.NATS.Cluster.Host,
		ClusterPort:      cfg.NATS.Cluster.Port,
		ClusterAdvertise: clusterAdv,
		TLS:              tlsLoader.Server(),
		ClusterTLS:       tlsLoader.Cluster(),
		StaticRoutes:     cfg.NATS.Discovery.StaticRoutes,
		Reader:           reader,
		Logger:           slogLoggerAdapter{log: log},
	})
	if err := server.Start(ctx); err != nil {
		return fmt.Errorf("bus start: %w", err)
	}
	defer server.Stop()
	notifyReady()

	enricher := &enrich.Enricher{
		Cluster: clusterName,
		Node:    nodeName,
		Reader:  reader,
		TasksRoot: cfg.Watch.TasksDir,
	}

	out := make(chan events.CloudEvent, 1024)

	// Lifecycle watcher.
	go runWatcher(ctx, cfg, log, enricher, out)

	// Snapshot emitter.
	if cfg.Snapshot.Enabled {
		emitter := &snapshot.Emitter{
			Cluster:  clusterName,
			Node:     nodeName,
			Interval: cfg.Snapshot.Interval,
			JitterPC: cfg.Snapshot.JitterPC,
			Reader:   reader,
			Probe:    snapshot.Prober{UseQMP: cfg.Snapshot.UseQMP},
			Out:      out,
		}
		go func() { _ = emitter.Run(ctx) }()

		// On-demand interrogation: respond to cluster- and node-scoped requests.
		clusterSub := events.SubjectSnapshotRequestCluster(clusterName)
		nodeSub := events.SubjectSnapshotRequestNode(clusterName, nodeName)
		for _, subj := range []string{clusterSub, nodeSub} {
			s := subj
			_, err := server.Conn().Subscribe(s, func(_ *natsMsg) {
				log.Info("snapshot requested", "subject", s)
				emitter.EmitOnce()
			})
			if err != nil {
				log.Warn("snapshot subscribe failed", "subject", s, "err", err)
			}
		}
	}

	// Publisher loop.
	for {
		select {
		case <-ctx.Done():
			log.Info("shutdown")
			return nil
		case ev := <-out:
			subject := subjectFor(ev)
			if err := server.PublishJSON(subject, ev); err != nil {
				log.Warn("publish failed", "subject", subject, "err", err)
			}
		}
	}
}

func runWatcher(ctx context.Context, cfg config.Config, log *slog.Logger, enricher *enrich.Enricher, out chan<- events.CloudEvent) {
	kinds := make([]events.Kind, 0, len(cfg.Watch.Include))
	for _, k := range cfg.Watch.Include {
		kinds = append(kinds, events.Kind(k))
	}
	w := tasks.NewWatcher(cfg.Watch.TasksDir, kinds)
	go func() {
		if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("tasks watcher exited", "err", err)
		}
	}()
	starts := map[string]time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-w.Events():
			if ev.Phase == tasks.PhaseStarted {
				starts[ev.UPID.Raw] = ev.ObsTime
			}
			start := starts[ev.UPID.Raw]
			ce := enricher.Lifecycle(ev, start)
			out <- ce
			if ev.Phase != tasks.PhaseStarted {
				delete(starts, ev.UPID.Raw)
			}
		}
	}
}

// subjectFor turns a CloudEvent into the canonical NATS subject for publishing.
func subjectFor(ev events.CloudEvent) string {
	d := ev.Data
	switch ev.Type {
	case events.TypeSnapshotComplete():
		return events.SubjectSnapshotComplete(d.Cluster, d.Node)
	default:
		return events.SubjectLifecycle(d.Cluster, d.Node, d.Kind, d.VMID, d.Action, d.Phase)
	}
}

func nonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// detectClusterName reads /etc/pve/.members and returns the cluster name.
// It retries briefly to avoid transient startup races with pmxcfs mount timing.
func detectClusterName(reader *pmxcfs.Reader, log *slog.Logger) string {
	const (
		maxAttempts = 20
		retryDelay  = 250 * time.Millisecond
	)
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		members, err := reader.Members()
		if err == nil {
			name := strings.TrimSpace(members.Cluster.Name)
			if name != "" {
				return name
			}
			log.Info("cluster name missing in .members; treating host as standalone", "pmxcfs_root", reader.Root())
			return ""
		}
		if attempt == maxAttempts {
			log.Warn("cluster name auto-detect failed; treating host as standalone",
				"pmxcfs_root", reader.Root(), "attempts", maxAttempts, "err", err)
			return ""
		}
		time.Sleep(retryDelay)
	}
	return ""
}

// resolveAdvertise returns effective ClientAdvertise/ClusterAdvertise strings.
//
// When the listener Host is a wildcard (0.0.0.0 / ::) and no explicit
// advertise is configured, nats-server gossips a useless URL and logs
// `Address "0.0.0.0" can not be resolved properly`. We pull the local node
// IP from /etc/pve/.members to produce a reachable advertise. Explicit
// per-listener config wins over auto-derivation.
func resolveAdvertise(cfg config.Config, reader *pmxcfs.Reader, log *slog.Logger) (clientAdv, clusterAdv string) {
	clientAdv = cfg.NATS.Client.Advertise
	clusterAdv = cfg.NATS.Cluster.Advertise
	if clientAdv != "" && clusterAdv != "" {
		return
	}
	ip, err := reader.LocalIP()
	if err != nil {
		log.Warn("advertise auto-detect failed; falling back to listener host",
			"err", err)
		return
	}
	if clientAdv == "" {
		clientAdv = fmt.Sprintf("%s:%d", ip, cfg.NATS.Client.Port)
	}
	if clusterAdv == "" {
		clusterAdv = fmt.Sprintf("%s:%d", ip, cfg.NATS.Cluster.Port)
	}
	log.Info("nats advertise resolved",
		"client", clientAdv, "cluster", clusterAdv)
	return
}

// slogLoggerAdapter satisfies bus.Logger using slog.
type slogLoggerAdapter struct{ log *slog.Logger }

func (s slogLoggerAdapter) Debug(msg string, args ...any) { s.log.Debug(msg, args...) }
func (s slogLoggerAdapter) Info(msg string, args ...any)  { s.log.Info(msg, args...) }
func (s slogLoggerAdapter) Warn(msg string, args ...any)  { s.log.Warn(msg, args...) }
func (s slogLoggerAdapter) Error(msg string, args ...any) { s.log.Error(msg, args...) }

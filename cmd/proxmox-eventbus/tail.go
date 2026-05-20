package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/config"
	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/tlsutil"
	nats "github.com/nats-io/nats.go"
)

// runTail subscribes to the local NATS server with a freshly minted, in-memory
// client certificate and prints CloudEvent JSON to stdout. Intended to be
// invoked over SSH ("ssh root@pve01 proxmox-eventbus tail pve.>") for quick
// live tailing without provisioning long-lived consumer credentials.
func runTail(args []string) int {
	fs := flag.NewFlagSet("tail", flag.ContinueOnError)
	cfgPath := fs.String("config", config.DefaultPath, "config file path")
	server := fs.String("server", "tls://127.0.0.1:4222", "NATS URL to connect to")
	cn := fs.String("cn", "proxmox-eventbus/tail", "CN for the ephemeral client cert")
	validity := fs.Duration("validity", time.Hour, "ephemeral client cert lifetime")
	jsonMode := fs.Bool("json", false, "emit raw CloudEvent JSON (one per line); default is human-readable summary")
	skipServerVerify := fs.Bool("insecure-skip-verify", true, "skip TLS hostname check on the server cert; on by default since pve-ssl.pem may not list 127.0.0.1")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: proxmox-eventbus tail [flags] <subject> [<subject>...]")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Subscribes to the local NATS server using an ephemeral client cert")
		fmt.Fprintln(os.Stderr, "minted from /etc/pve/pve-root-ca.* (must run as root).")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Examples:")
		fmt.Fprintln(os.Stderr, "  proxmox-eventbus tail 'pve.>'")
		fmt.Fprintln(os.Stderr, "  proxmox-eventbus tail 'pve.NOV.*.qemu.*.migrate.>'")
		fmt.Fprintln(os.Stderr, "  proxmox-eventbus tail --json 'pve.NOV.>' | jq .")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Flags:")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	subjects := fs.Args()
	if len(subjects) == 0 {
		fs.Usage()
		return 2
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		return 1
	}

	tlsCfg, err := buildTailTLS(cfg, *cn, *validity, *skipServerVerify)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tls setup:", err)
		return 1
	}

	nc, err := nats.Connect(*server,
		nats.Secure(tlsCfg),
		nats.Name("proxmox-eventbus/tail"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			fmt.Fprintln(os.Stderr, "nats:", err)
		}),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect:", err)
		return 1
	}
	defer nc.Close()

	for _, subj := range subjects {
		_, err := nc.Subscribe(subj, makeHandler(*jsonMode))
		if err != nil {
			fmt.Fprintln(os.Stderr, "subscribe", subj, ":", err)
			return 1
		}
	}
	if err := nc.Flush(); err != nil {
		fmt.Fprintln(os.Stderr, "flush:", err)
		return 1
	}

	fmt.Fprintf(os.Stderr, "tailing %v from %s (ctrl-c to stop)\n",
		subjects, *server)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	<-ctx.Done()
	return 0
}

func buildTailTLS(cfg config.Config, cn string, validity time.Duration, skipVerify bool) (*tls.Config, error) {
	bundle, err := tlsutil.IssueClientCertPEM(tlsutil.IssueClientCertOpts{
		CACertFile: cfg.NATS.TLS.CAFile,
		CAKeyFile:  "", // tlsutil falls back to DefaultCAkeyPath
		CN:         cn,
		Validity:   validity,
	})
	if err != nil {
		return nil, err
	}
	cert, err := tls.X509KeyPair(bundle.CertPEM, bundle.KeyPEM)
	if err != nil {
		return nil, fmt.Errorf("load minted cert: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(bundle.CAPEM) {
		return nil, errors.New("no PEM blocks in cluster CA")
	}
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		Certificates:       []tls.Certificate{cert},
		RootCAs:            pool,
		InsecureSkipVerify: skipVerify,
	}, nil
}

func makeHandler(jsonMode bool) nats.MsgHandler {
	if jsonMode {
		return func(m *nats.Msg) {
			_, _ = os.Stdout.Write(m.Data)
			_, _ = io.WriteString(os.Stdout, "\n")
		}
	}
	return func(m *nats.Msg) {
		printSummary(m)
	}
}

// printSummary renders one human-readable line per event:
//
//	HH:MM:SS  <subject>  action=<a> phase=<p> [migrate-target=<n>] [duration=<ms>]
//
// The full JSON is appended in dimmed grey at the end so jq-style pipelines
// still work.
func printSummary(m *nats.Msg) {
	var ev struct {
		Time time.Time `json:"time"`
		Data struct {
			Action     string `json:"action"`
			Phase      string `json:"phase"`
			State      string `json:"state"`
			VMID       int    `json:"vmid"`
			Name       string `json:"name"`
			TargetNode string `json:"target_node"`
			DurationMS *int64 `json:"duration_ms"`
			ExitStatus string `json:"exit_status"`
		} `json:"data"`
	}
	if err := json.Unmarshal(m.Data, &ev); err != nil {
		_, _ = fmt.Fprintf(os.Stdout, "%s  %s  (unparsed) %s\n",
			time.Now().Format("15:04:05"), m.Subject, m.Data)
		return
	}

	var b strings.Builder
	b.WriteString(ev.Time.Local().Format("15:04:05"))
	b.WriteString("  ")
	b.WriteString(m.Subject)
	if ev.Data.VMID != 0 {
		fmt.Fprintf(&b, "  vm=%d", ev.Data.VMID)
		if ev.Data.Name != "" {
			fmt.Fprintf(&b, "(%s)", ev.Data.Name)
		}
	}
	if ev.Data.Action != "" {
		fmt.Fprintf(&b, "  %s/%s", ev.Data.Action, ev.Data.Phase)
	} else if ev.Data.State != "" {
		fmt.Fprintf(&b, "  state=%s", ev.Data.State)
	}
	if ev.Data.TargetNode != "" {
		fmt.Fprintf(&b, "  -> %s", ev.Data.TargetNode)
	}
	if ev.Data.DurationMS != nil {
		fmt.Fprintf(&b, "  took=%dms", *ev.Data.DurationMS)
	}
	if ev.Data.ExitStatus != "" && ev.Data.ExitStatus != "OK" {
		fmt.Fprintf(&b, "  exit=%q", ev.Data.ExitStatus)
	}
	b.WriteByte('\n')
	_, _ = os.Stdout.WriteString(b.String())
}

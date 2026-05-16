package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load empty: %v", err)
	}
	if cfg.Snapshot.Interval != 30*time.Second {
		t.Errorf("default interval = %v, want 30s", cfg.Snapshot.Interval)
	}
	if cfg.NATS.TLS.CAFile != "/etc/pve/pve-root-ca.pem" {
		t.Errorf("default CA = %q", cfg.NATS.TLS.CAFile)
	}
}

func TestLoadOverlay(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "c.yaml")
	yaml := `
snapshot:
  interval: 5s
  jitter_percent: 25
  qmp: true
nats:
  client:
    port: 4333
  discovery:
    static_routes: ["nats-route://10.0.0.99:6222"]
logging:
  level: debug
  format: json
`
	if err := os.WriteFile(p, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Snapshot.Interval != 5*time.Second {
		t.Errorf("interval = %v", cfg.Snapshot.Interval)
	}
	if !cfg.Snapshot.UseQMP {
		t.Error("qmp not parsed")
	}
	if cfg.NATS.Client.Port != 4333 {
		t.Errorf("client port = %d", cfg.NATS.Client.Port)
	}
	if got := cfg.NATS.Discovery.StaticRoutes; len(got) != 1 || got[0] != "nats-route://10.0.0.99:6222" {
		t.Errorf("static_routes = %v", got)
	}
}

func TestValidate(t *testing.T) {
	cfg := Defaults()
	cfg.Snapshot.JitterPC = 200
	if err := cfg.Validate(); err == nil {
		t.Error("expected validation error for jitter 200")
	}
	cfg = Defaults()
	cfg.Logging.Format = "bogus"
	if err := cfg.Validate(); err == nil {
		t.Error("expected validation error for bad format")
	}
}

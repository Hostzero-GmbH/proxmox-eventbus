// Package config loads and validates the daemon configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const DefaultPath = "/etc/proxmox-eventbus/config.yaml"

type Config struct {
	Node     NodeConfig     `yaml:"node"`
	Watch    WatchConfig    `yaml:"watch"`
	Snapshot SnapshotConfig `yaml:"snapshot"`
	NATS     NATSConfig     `yaml:"nats"`
	Logging  LoggingConfig  `yaml:"logging"`
}

type NodeConfig struct {
	// ClusterName overrides the cluster name; empty = auto from /etc/pve/.members.
	ClusterName string `yaml:"cluster_name"`
	// Name overrides the local node name; empty = read /etc/hostname.
	Name string `yaml:"name"`
	// PmxcfsRoot lets tests point at a fake /etc/pve.
	PmxcfsRoot string `yaml:"pmxcfs_root"`
}

type WatchConfig struct {
	TasksDir string   `yaml:"tasks_dir"`
	Include  []string `yaml:"include"` // "qemu", "lxc"
	Actions  []string `yaml:"actions"` // empty = all
}

type SnapshotConfig struct {
	Enabled  bool          `yaml:"enabled"`
	Interval time.Duration `yaml:"interval"`
	JitterPC int           `yaml:"jitter_percent"` // 0-100
	UseQMP   bool          `yaml:"qmp"`            // off by default; precise state via QMP query-status
}

type NATSConfig struct {
	ServerName string         `yaml:"server_name"` // defaults to node name
	Client     ListenerConfig `yaml:"client"`
	Cluster    ListenerConfig `yaml:"cluster"`
	TLS        TLSConfig      `yaml:"tls"`
	Discovery  DiscoveryConfig `yaml:"discovery"`
}

type ListenerConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
	// Advertise is the host:port (or bare host) gossiped to peers/clients.
	// Empty = auto-derive "<local-ip>:<port>" from /etc/pve/.members so that
	// 0.0.0.0 listeners still gossip a reachable address; falls back to no
	// advertise if the IP can't be resolved.
	Advertise string `yaml:"advertise"`
}

type TLSConfig struct {
	CAFile   string `yaml:"ca_file"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	// VerifyClient enforces mTLS on the client listener. Cluster listener always verifies.
	VerifyClient bool `yaml:"verify_client"`
}

type DiscoveryConfig struct {
	// MembersFile is read for the cluster route list. Defaults to /etc/pve/.members.
	MembersFile string `yaml:"members_file"`
	// StaticRoutes are added on top of discovered peers (e.g. nats-route://1.2.3.4:6222).
	StaticRoutes []string `yaml:"static_routes"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`  // debug|info|warn|error
	Format string `yaml:"format"` // journald|json|text
}

func Defaults() Config {
	return Config{
		Watch: WatchConfig{
			TasksDir: "/var/log/pve/tasks",
			Include:  []string{"qemu", "lxc"},
		},
		Snapshot: SnapshotConfig{
			Enabled:  true,
			Interval: 30 * time.Second,
			JitterPC: 10,
			UseQMP:   false,
		},
		NATS: NATSConfig{
			Client: ListenerConfig{
				Host: "0.0.0.0",
				Port: 4222,
			},
			Cluster: ListenerConfig{
				Host: "0.0.0.0",
				Port: 6222,
			},
			TLS: TLSConfig{
				CAFile:       "/etc/pve/pve-root-ca.pem",
				CertFile:     "/etc/pve/local/pve-ssl.pem",
				KeyFile:      "/etc/pve/local/pve-ssl.key",
				VerifyClient: true,
			},
			Discovery: DiscoveryConfig{
				MembersFile: "/etc/pve/.members",
			},
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "journald",
		},
		Node: NodeConfig{
			PmxcfsRoot: "/etc/pve",
		},
	}
}

func Load(path string) (Config, error) {
	cfg := Defaults()
	if path == "" {
		return cfg, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return cfg, nil
		}
		return cfg, fmt.Errorf("read %s: %w", path, err)
	}
	dec := yaml.NewDecoder(bytesReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.Snapshot.Interval < 0 {
		return errors.New("snapshot.interval must be >= 0")
	}
	if c.Snapshot.JitterPC < 0 || c.Snapshot.JitterPC > 100 {
		return errors.New("snapshot.jitter_percent must be 0-100")
	}
	if c.Logging.Format != "" {
		switch c.Logging.Format {
		case "journald", "json", "text":
		default:
			return fmt.Errorf("logging.format %q: must be journald|json|text", c.Logging.Format)
		}
	}
	for _, k := range c.Watch.Include {
		if k != "qemu" && k != "lxc" {
			return fmt.Errorf("watch.include %q: must be qemu or lxc", k)
		}
	}
	return nil
}

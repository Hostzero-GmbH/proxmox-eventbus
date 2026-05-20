package pmxcfs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadMembers(t *testing.T) {
	dir := t.TempDir()
	payload := `{
  "nodename": "pve1",
  "version": 4,
  "cluster": { "name": "hzero", "version": 3, "nodes": 3, "quorate": 1 },
  "nodelist": {
    "pve1": { "id": 1, "online": 1, "ip": "10.0.0.1" },
    "pve2": { "id": 2, "online": 1, "ip": "10.0.0.2" },
    "pve3": { "id": 3, "online": 0, "ip": "10.0.0.3" }
  }
}`
	if err := os.WriteFile(filepath.Join(dir, ".members"), []byte(payload), 0o644); err != nil {
		t.Fatal(err)
	}
	r := New(dir)
	m, err := r.Members()
	if err != nil {
		t.Fatalf("Members: %v", err)
	}
	if m.Cluster.Name != "hzero" {
		t.Errorf("cluster name = %q", m.Cluster.Name)
	}
	peers, err := r.Peers()
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 1 {
		t.Errorf("peers = %v, want 1 online non-self", peers)
	}
	if peers[0].IP != "10.0.0.2" {
		t.Errorf("peer IP = %q", peers[0].IP)
	}
	ip, err := r.LocalIP()
	if err != nil {
		t.Fatalf("LocalIP: %v", err)
	}
	if ip != "10.0.0.1" {
		t.Errorf("LocalIP = %q, want 10.0.0.1", ip)
	}
}

func TestReadVMConfig(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "qemu-server"), 0o755); err != nil {
		t.Fatal(err)
	}
	conf := `name: web-01
tags: prod;floating-ip
description: a sample\nmulti-line desc
net0: virtio=AA:BB:CC:DD:EE:FF,bridge=vmbr0,tag=10
hookscript: local:snippets/migrate.sh

[snapshot snap1]
name: web-01-snap
`
	if err := os.WriteFile(filepath.Join(dir, "qemu-server", "101.conf"), []byte(conf), 0o644); err != nil {
		t.Fatal(err)
	}
	r := New(dir)
	c, err := r.ReadVMConfig(KindQEMU, 101)
	if err != nil {
		t.Fatalf("ReadVMConfig: %v", err)
	}
	if c.Name != "web-01" {
		t.Errorf("name = %q", c.Name)
	}
	if got := c.Tags; len(got) != 2 || got[0] != "prod" || got[1] != "floating-ip" {
		t.Errorf("tags = %v", got)
	}
	if got, want := c.Net["net0"], "virtio=AA:BB:CC:DD:EE:FF,bridge=vmbr0,tag=10"; got != want {
		t.Errorf("net0 = %q", got)
	}
	if c.HookScript != "local:snippets/migrate.sh" {
		t.Errorf("hookscript = %q", c.HookScript)
	}
	if c.Description == "a sample\\nmulti-line desc" {
		t.Errorf("description not unescaped: %q", c.Description)
	}
}

func TestVMList(t *testing.T) {
	dir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dir, "qemu-server"), 0o755)
	_ = os.MkdirAll(filepath.Join(dir, "lxc"), 0o755)
	_ = os.WriteFile(filepath.Join(dir, "qemu-server", "101.conf"), []byte("name: a\n"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, "qemu-server", "102.conf"), []byte("name: b\n"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, "lxc", "200.conf"), []byte("hostname: c\n"), 0o644)
	q, l, err := New(dir).VMList()
	if err != nil {
		t.Fatal(err)
	}
	if len(q) != 2 || len(l) != 1 {
		t.Errorf("got qemu=%v lxc=%v", q, l)
	}
}

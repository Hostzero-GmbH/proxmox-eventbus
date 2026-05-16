package enrich

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/events"
	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/pmxcfs"
	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/tasks"
)

func TestLifecycleMigrate(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "qemu-server"), 0o755); err != nil {
		t.Fatal(err)
	}
	conf := "name: web-01\ntags: prod;floating-ip\n"
	if err := os.WriteFile(filepath.Join(dir, "qemu-server", "101.conf"), []byte(conf), 0o644); err != nil {
		t.Fatal(err)
	}

	// Synthesise a /proc/<pid>/cmdline containing `qm migrate 101 pve2 --online`.
	proc := t.TempDir()
	pid := 12345
	cmdDir := filepath.Join(proc, fmt.Sprintf("%d", pid))
	if err := os.MkdirAll(cmdDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cmdDir, "cmdline"),
		[]byte("/usr/sbin/qm\x00migrate\x00101\x00pve2\x00--online\x00"),
		0o644); err != nil {
		t.Fatal(err)
	}

	upid, err := tasks.ParseUPID(fmt.Sprintf("UPID:pve1:%08x:0ABCDEF0:6645A1B2:qmigrate:101:root@pam:", pid))
	if err != nil {
		t.Fatal(err)
	}
	e := &Enricher{
		Cluster:  "hzero",
		Node:     "pve1",
		Reader:   pmxcfs.New(dir),
		ProcRoot: proc,
	}
	ev := e.Lifecycle(tasks.Event{
		UPID: upid, Kind: events.KindQEMU, Action: events.ActionMigrate, Phase: tasks.PhaseStarted,
	}, time.Time{})

	if ev.Data.TargetNode != "pve2" {
		t.Errorf("target_node = %q", ev.Data.TargetNode)
	}
	if ev.Data.Name != "web-01" {
		t.Errorf("name = %q", ev.Data.Name)
	}
	if got := ev.Data.Tags; len(got) != 2 {
		t.Errorf("tags = %v", got)
	}
	if ev.Type != "dev.proxmox.eventbus.qemu.migrate.started" {
		t.Errorf("type = %q", ev.Type)
	}
}

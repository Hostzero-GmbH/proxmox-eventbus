package snapshot

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/events"
	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/pmxcfs"
)

func TestEmitOnce(t *testing.T) {
	dir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dir, "qemu-server"), 0o755)
	_ = os.MkdirAll(filepath.Join(dir, "lxc"), 0o755)
	_ = os.WriteFile(filepath.Join(dir, "qemu-server", "101.conf"), []byte("name: web\n"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, "lxc", "200.conf"), []byte("hostname: ct\n"), 0o644)

	run := t.TempDir()
	_ = os.MkdirAll(filepath.Join(run, "qemu-server"), 0o755)
	pid := os.Getpid()
	_ = os.WriteFile(filepath.Join(run, "qemu-server", "101.pid"),
		[]byte(strconv.Itoa(pid)), 0o644)

	cgroup := t.TempDir()
	_ = os.MkdirAll(filepath.Join(cgroup, "lxc", "200"), 0o755)
	_ = os.WriteFile(filepath.Join(cgroup, "lxc", "200", "cgroup.procs"), []byte("123\n"), 0o644)
	proc := t.TempDir()
	_ = os.MkdirAll(filepath.Join(proc, strconv.Itoa(pid)), 0o755)

	out := make(chan events.CloudEvent, 16)
	e := &Emitter{
		Cluster: "hzero",
		Node:    "pve1",
		Reader:  pmxcfs.New(dir),
		Probe:   Prober{RunDir: run, CgroupV2: cgroup, ProcRoot: proc},
		Out:     out,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = e.Run(ctx) }() // Run with Interval=0 emits once and returns

	deadline := time.After(2 * time.Second)
	gotQEMU, gotLXC, gotComplete := false, false, false
	var snapID string
	for !(gotQEMU && gotLXC && gotComplete) {
		select {
		case ev := <-out:
			switch {
			case ev.Type == events.TypeSnapshotComplete():
				gotComplete = true
				snapID = ev.Data.SnapshotID
			case ev.Data.Kind == events.KindQEMU && ev.Data.State == events.StateRunning:
				gotQEMU = true
				if ev.Data.SnapshotID == "" {
					t.Error("missing snapshot_id on qemu event")
				}
			case ev.Data.Kind == events.KindLXC && ev.Data.State == events.StateRunning:
				gotLXC = true
			}
		case <-deadline:
			t.Fatalf("timed out: qemu=%v lxc=%v complete=%v", gotQEMU, gotLXC, gotComplete)
		}
	}
	if snapID == "" {
		t.Error("snapshot.complete missing snapshot_id")
	}
}


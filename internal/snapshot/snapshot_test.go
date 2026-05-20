package snapshot

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
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
	for !gotQEMU || !gotLXC || !gotComplete {
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

// TestQEMUStateUnreadablePIDFile reproduces the production setup: PVE creates
// /run/qemu-server/<vmid>.pid as mode 0600 root:root, and our daemon runs as
// an unprivileged user that can stat the file but not read it. The state must
// be Running (file existence is authoritative), not Unknown.
func TestQEMUStateUnreadablePIDFile(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("unix-only: relies on file mode bits")
	}
	if os.Geteuid() == 0 {
		t.Skip("root bypasses mode 0000 - mode bit semantics don't apply here")
	}
	run := t.TempDir()
	if err := os.MkdirAll(filepath.Join(run, "qemu-server"), 0o755); err != nil {
		t.Fatal(err)
	}
	pidPath := filepath.Join(run, "qemu-server", "200.pid")
	if err := os.WriteFile(pidPath, []byte("12345\n"), 0o000); err != nil {
		t.Fatal(err)
	}
	// Sanity: opening must fail with EACCES.
	if _, err := os.Open(pidPath); err == nil {
		t.Fatalf("expected EACCES on mode 0000 file, got nil")
	}

	state, detail := Prober{RunDir: run}.QEMUState(200)
	if state != events.StateRunning {
		t.Errorf("state = %s (%q), want running", state, detail)
	}
	if detail != "" {
		t.Errorf("detail = %q, want empty for happy path", detail)
	}
}

// TestQEMUStateEmptyPIDFile covers the "PVE crashed mid-startup" case: the
// PID file exists but is empty/garbage, so the VM is effectively stopped.
func TestQEMUStateEmptyPIDFile(t *testing.T) {
	run := t.TempDir()
	if err := os.MkdirAll(filepath.Join(run, "qemu-server"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, content := range []string{"", "  \n", "not-a-pid", "abc\n"} {
		pidPath := filepath.Join(run, "qemu-server", "300.pid")
		if err := os.WriteFile(pidPath, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		state, detail := Prober{RunDir: run}.QEMUState(300)
		if state != events.StateStopped {
			t.Errorf("content %q: state = %s (%q), want stopped", content, state, detail)
		}
	}
}

// TestQEMUStateMissingPIDFile covers the normal stopped case.
func TestQEMUStateMissingPIDFile(t *testing.T) {
	run := t.TempDir()
	state, detail := Prober{RunDir: run}.QEMUState(404)
	if state != events.StateStopped {
		t.Errorf("state = %s (%q), want stopped", state, detail)
	}
}


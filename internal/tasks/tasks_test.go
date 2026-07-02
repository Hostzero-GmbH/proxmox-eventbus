package tasks

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/events"
)

func TestParseUPID(t *testing.T) {
	in := "UPID:pve1:00001234:0ABCDEF0:6645A1B2:qmigrate:101:root@pam:"
	u, err := ParseUPID(in)
	if err != nil {
		t.Fatalf("ParseUPID: %v", err)
	}
	if u.Node != "pve1" || u.PID != 0x1234 || u.Type != "qmigrate" || u.ID != "101" || u.User != "root@pam" {
		t.Errorf("bad parse: %+v", u)
	}
	if u.VMID() != 101 {
		t.Errorf("vmid = %d", u.VMID())
	}
	if !u.IsLifecycle() {
		t.Errorf("expected lifecycle for qmigrate")
	}
}

func TestParseActiveLine(t *testing.T) {
	upid := "UPID:pve1:00001234:0ABCDEF0:6645A1B2:qmstart:101:root@pam:"
	cases := []struct {
		name       string
		line       string
		wantStatus string
		wantRaw    string // expected upid.Raw
	}{
		{"bare upid", upid, "", upid},
		{"active running", upid + " 0", "", upid},
		{"active finished OK", upid + " 1 6645A1B7 OK", "OK", upid},
		{"active failed", upid + " 1 6645A1B7 command 'qm start' failed: exit code 1", "command 'qm start' failed: exit code 1", upid},
		{"index entry", upid + " 6645A1B7 OK", "OK", upid},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			u, status, err := ParseActiveLine(c.line)
			if err != nil {
				t.Fatalf("parse %q: %v", c.line, err)
			}
			if u.Raw != c.wantRaw {
				t.Errorf("Raw = %q, want %q", u.Raw, c.wantRaw)
			}
			if status != c.wantStatus {
				t.Errorf("status = %q, want %q", status, c.wantStatus)
			}
		})
	}
}

func TestWorkerTypeMap(t *testing.T) {
	cases := []struct {
		in     string
		kind   events.Kind
		action events.Action
	}{
		{"qmstart", events.KindQEMU, events.ActionStart},
		{"vzmigrate", events.KindLXC, events.ActionMigrate},
	}
	for _, c := range cases {
		k, a, ok := WorkerTypeMap(c.in)
		if !ok || k != c.kind || a != c.action {
			t.Errorf("%s: got (%s,%s,%v)", c.in, k, a, ok)
		}
	}
	if _, _, ok := WorkerTypeMap("vzdump"); ok {
		t.Errorf("vzdump should not be a lifecycle action")
	}
}

func TestWatcherEndToEnd(t *testing.T) {
	dir := t.TempDir()
	active := filepath.Join(dir, "active")
	if err := os.WriteFile(active, []byte{}, 0o644); err != nil {
		t.Fatal(err)
	}

	w := NewWatcher(dir, nil)
	w.debounce = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- w.Run(ctx) }()
	time.Sleep(20 * time.Millisecond)

	upid := "UPID:pve1:00001234:0ABCDEF0:6645A1B2:qmigrate:101:root@pam:"
	if err := os.WriteFile(active, []byte(upid+" 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	got := receive(t, w.Events(), 500*time.Millisecond)
	if got.Phase != PhaseStarted || got.Action != events.ActionMigrate {
		t.Errorf("started: %+v", got)
	}

	if err := os.WriteFile(active, []byte(upid+" 1 6645A1B7 OK\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got = receive(t, w.Events(), 500*time.Millisecond)
	if got.Phase != PhaseFinished {
		t.Errorf("finished: %+v", got)
	}
}

// TestWatcherNoDuplicateStarted regression-tests the parser bug where the
// running form (`<UPID> 0`) and the terminated form (`<UPID> 1 <hex> OK`)
// were treated as distinct UPIDs - producing two `started` events and zero
// `finished` events per real task.
func TestWatcherNoDuplicateStarted(t *testing.T) {
	dir := t.TempDir()
	active := filepath.Join(dir, "active")
	if err := os.WriteFile(active, []byte{}, 0o644); err != nil {
		t.Fatal(err)
	}

	w := NewWatcher(dir, nil)
	w.debounce = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()
	time.Sleep(20 * time.Millisecond)

	upid := "UPID:pve1:00001234:0ABCDEF0:6645A1B2:qmstop:101:root@pam:"
	if err := os.WriteFile(active, []byte(upid+" 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	first := receive(t, w.Events(), 500*time.Millisecond)
	if first.Phase != PhaseStarted {
		t.Fatalf("first: phase=%s want started", first.Phase)
	}

	if err := os.WriteFile(active, []byte(upid+" 1 6645A1B7 OK\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	second := receive(t, w.Events(), 500*time.Millisecond)
	if second.Phase != PhaseFinished || second.Status != "OK" {
		t.Errorf("second: phase=%s status=%q want finished/OK", second.Phase, second.Status)
	}

	select {
	case extra := <-w.Events():
		t.Fatalf("unexpected third event: %+v", extra)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestWatcherMigrationSyncedPhase regression-tests that a live migration's
// per-task log line "migration status: completed" produces a PhaseSynced
// event well before the task's terminal status lands in the active file -
// this is the signal consumers actually want for a fast VM handover, since
// PVE still has several seconds of NBD/conntrack cleanup left at that point.
func TestWatcherMigrationSyncedPhase(t *testing.T) {
	dir := t.TempDir()
	active := filepath.Join(dir, "active")
	if err := os.WriteFile(active, []byte{}, 0o644); err != nil {
		t.Fatal(err)
	}

	w := NewWatcher(dir, nil)
	w.debounce = 5 * time.Millisecond
	w.syncPollInterval = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()
	time.Sleep(20 * time.Millisecond)

	upid := "UPID:pve1:00001234:0ABCDEF0:6645A1B2:qmigrate:999:root@pam:"
	if err := os.WriteFile(active, []byte(upid+" 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	started := receive(t, w.Events(), 500*time.Millisecond)
	if started.Phase != PhaseStarted || started.Action != events.ActionMigrate {
		t.Fatalf("started: %+v", started)
	}

	logFile := filepath.Join(dir, strings.TrimSuffix(upid, ":"))
	if err := os.WriteFile(logFile, []byte("starting migration of VM 999 ...\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	select {
	case ev := <-w.Events():
		t.Fatalf("unexpected event before marker written: %+v", ev)
	case <-time.After(30 * time.Millisecond):
	}

	if err := appendFile(t, logFile, "2026-07-01 13:55:38 migration status: completed\n"); err != nil {
		t.Fatal(err)
	}

	synced := receive(t, w.Events(), 500*time.Millisecond)
	if synced.Phase != PhaseSynced || synced.Action != events.ActionMigrate {
		t.Fatalf("synced: %+v", synced)
	}

	if err := os.WriteFile(active, []byte(upid+" 1 6645A1B7 OK\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	finished := receive(t, w.Events(), 500*time.Millisecond)
	if finished.Phase != PhaseFinished || finished.Status != "OK" {
		t.Fatalf("finished: %+v", finished)
	}

	select {
	case extra := <-w.Events():
		t.Fatalf("unexpected extra event: %+v", extra)
	case <-time.After(100 * time.Millisecond):
	}
}

func appendFile(t *testing.T, path, s string) error {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(s)
	return err
}

func receive(t *testing.T, ch <-chan Event, d time.Duration) Event {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(d):
		t.Fatal("timed out waiting for event")
		return Event{}
	}
}

package tasks

import (
	"context"
	"os"
	"path/filepath"
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

func TestParseActiveLineWithStatus(t *testing.T) {
	line := "UPID:pve1:00001234:0ABCDEF0:6645A1B2:qmstart:101:root@pam:\tOK"
	u, status, err := ParseActiveLine(line)
	if err != nil {
		t.Fatal(err)
	}
	if u.Type != "qmstart" || status != "OK" {
		t.Errorf("got u.Type=%q status=%q", u.Type, status)
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
	if err := os.WriteFile(active, []byte(upid+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	got := receive(t, w.Events(), 500*time.Millisecond)
	if got.Phase != PhaseStarted || got.Action != events.ActionMigrate {
		t.Errorf("started: %+v", got)
	}

	if err := os.WriteFile(active, []byte(upid+"\tOK\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got = receive(t, w.Events(), 500*time.Millisecond)
	if got.Phase != PhaseFinished {
		t.Errorf("finished: %+v", got)
	}
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

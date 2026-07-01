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

func TestLifecycleMigrateQmigrateBinary(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "qemu-server"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "qemu-server", "101.conf"), []byte("name: web-01\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	proc := t.TempDir()
	pid := 22222
	cmdDir := filepath.Join(proc, fmt.Sprintf("%d", pid))
	if err := os.MkdirAll(cmdDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cmdDir, "cmdline"),
		[]byte("/usr/sbin/qmigrate\x00101\x00pve2\x00--online\x00"),
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
}

func TestLifecycleMigrateFinishedUsesCachedTarget(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "qemu-server"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "qemu-server", "101.conf"), []byte("name: web-01\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	proc := t.TempDir()
	pid := 33333
	cmdDir := filepath.Join(proc, fmt.Sprintf("%d", pid))
	if err := os.MkdirAll(cmdDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cmdlinePath := filepath.Join(cmdDir, "cmdline")
	if err := os.WriteFile(cmdlinePath,
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

	started := e.Lifecycle(tasks.Event{
		UPID: upid, Kind: events.KindQEMU, Action: events.ActionMigrate, Phase: tasks.PhaseStarted,
	}, time.Time{})
	if started.Data.TargetNode != "pve2" {
		t.Fatalf("started target_node = %q", started.Data.TargetNode)
	}

	if err := os.Remove(cmdlinePath); err != nil {
		t.Fatal(err)
	}

	finished := e.Lifecycle(tasks.Event{
		UPID: upid,
		Kind: events.KindQEMU, Action: events.ActionMigrate,
		Phase: tasks.PhaseFinished, Status: "OK",
	}, time.Now().Add(-2*time.Second))

	if finished.Data.TargetNode != "pve2" {
		t.Errorf("finished target_node = %q", finished.Data.TargetNode)
	}
}

func TestParseMigrateTargetOptionAndKVForms(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "option target flag",
			args: []string{"/usr/bin/perl", "/usr/sbin/qm", "migrate", "999", "--online", "1", "--target", "pve01"},
			want: "pve01",
		},
		{
			name: "inline target kv",
			args: []string{"/usr/bin/perl", "/usr/sbin/qmigrate", "vmid=999", "target=pve01", "online=1"},
			want: "pve01",
		},
		{
			name: "migrate token kv tail",
			args: []string{"/usr/bin/perl", "something", "migrate", "vmid=999", "target_node=pve01"},
			want: "pve01",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseMigrateTarget(tc.args); got != tc.want {
				t.Fatalf("target=%q want %q", got, tc.want)
			}
		})
	}
}

func TestLifecycleMigrateFallsBackToTaskLog(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "qemu-server"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "qemu-server", "999.conf"), []byte("name: natvm\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	upidRaw := "UPID:pve02:00001234:0ABCDEF0:6645A1B2:qmigrate:999:root@pam:"
	upid, err := tasks.ParseUPID(upidRaw)
	if err != nil {
		t.Fatal(err)
	}

	tasksRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tasksRoot, "AA"), 0o755); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(tasksRoot, "AA", upidRaw)
	logBody := "2026-06-17 16:24:13 starting migration of VM 999 to node 'pve01'\n"
	if err := os.WriteFile(logPath, []byte(logBody), 0o644); err != nil {
		t.Fatal(err)
	}

	e := &Enricher{
		Cluster:   "NOV",
		Node:      "pve02",
		Reader:    pmxcfs.New(dir),
		ProcRoot:  t.TempDir(), // intentionally missing /proc/<pid>/cmdline
		TasksRoot: tasksRoot,
	}

	ev := e.Lifecycle(tasks.Event{
		UPID: upid, Kind: events.KindQEMU, Action: events.ActionMigrate, Phase: tasks.PhaseStarted,
	}, time.Time{})

	if ev.Data.TargetNode != "pve01" {
		t.Fatalf("target_node=%q want pve01", ev.Data.TargetNode)
	}
}

func TestParseMigrateTargetFromLog(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"starting migration of VM 999 to node 'pve01'", "pve01"},
		{"target: pve01", "pve01"},
		{"target_node=pve01", "pve01"},
	}

	for _, c := range cases {
		if got := parseMigrateTargetFromLog(c.in); got != c.want {
			t.Fatalf("got %q want %q for %q", got, c.want, c.in)
		}
	}
}

func TestLifecycleMigrateStartedWaitsForTaskLogTarget(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "qemu-server"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "qemu-server", "999.conf"), []byte("name: natvm\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	upidRaw := "UPID:pve01:00001234:0ABCDEF0:6645A1B2:qmigrate:999:root@pam:"
	upid, err := tasks.ParseUPID(upidRaw)
	if err != nil {
		t.Fatal(err)
	}

	tasksRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tasksRoot, "AA"), 0o755); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(tasksRoot, "AA", upidRaw)

	go func() {
		time.Sleep(60 * time.Millisecond)
		_ = os.WriteFile(logPath, []byte("starting migration of VM 999 to node 'pve02'\n"), 0o644)
	}()

	e := &Enricher{
		Cluster:           "NOV",
		Node:              "pve01",
		Reader:            pmxcfs.New(dir),
		ProcRoot:          t.TempDir(),
		TasksRoot:         tasksRoot,
		MigrateTargetWait: 300 * time.Millisecond,
		MigrateTargetPoll: 20 * time.Millisecond,
	}

	ev := e.Lifecycle(tasks.Event{
		UPID: upid, Kind: events.KindQEMU, Action: events.ActionMigrate, Phase: tasks.PhaseStarted,
	}, time.Time{})

	if ev.Data.TargetNode != "pve02" {
		t.Fatalf("target_node=%q want pve02", ev.Data.TargetNode)
	}
}

package tasks

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/events"
	"github.com/fsnotify/fsnotify"
)

// Phase reports whether the task just started or has terminated.
type Phase string

const (
	PhaseStarted  Phase = "started"
	PhaseFinished Phase = "finished"
	PhaseFailed   Phase = "failed"
	// PhaseSynced is a non-terminal phase emitted once for migrate tasks,
	// the moment the per-task log shows migrationSyncedMarker. See
	// watchMigrationSynced for details.
	PhaseSynced Phase = "synced"
)

// Event is the raw observation produced by the watcher; it is the input to internal/enrich.
type Event struct {
	UPID    UPID
	Kind    events.Kind
	Action  events.Action
	Phase   Phase
	Status  string // raw status string from the active file, e.g. "OK" or "command 'qm migrate ...' failed: exit code 1"
	ObsTime time.Time
}

// Watcher tails the PVE task active file and emits Events for each phase transition.
//
// Implementation note: PVE rewrites /var/log/pve/tasks/active in place when tasks
// finish. We can't tail it like a regular append-only log. Instead we re-read the
// file on every change and compare against the set of UPIDs we've already emitted
// a started/finished event for.
type Watcher struct {
	tasksDir string
	include  map[events.Kind]bool

	mu       sync.Mutex
	known    map[string]upidState // keyed by raw UPID
	debounce time.Duration
	// syncPollInterval controls how often watchMigrationSynced re-reads a
	// migrate task's log file looking for migrationSyncedMarker.
	syncPollInterval time.Duration

	out chan Event
}

type upidState struct {
	startedEmitted  bool
	finishedEmitted bool
	syncedEmitted   bool
}

// NewWatcher creates a Watcher rooted at tasksDir (typically /var/log/pve/tasks).
// include filters by kind ("qemu", "lxc"); empty = both.
func NewWatcher(tasksDir string, include []events.Kind) *Watcher {
	set := map[events.Kind]bool{}
	if len(include) == 0 {
		set[events.KindQEMU] = true
		set[events.KindLXC] = true
	} else {
		for _, k := range include {
			set[k] = true
		}
	}
	return &Watcher{
		tasksDir:         tasksDir,
		include:          set,
		known:            map[string]upidState{},
		debounce:         25 * time.Millisecond,
		syncPollInterval: 250 * time.Millisecond,
		out:              make(chan Event, 256),
	}
}

// Events returns the channel on which raw observations are delivered.
func (w *Watcher) Events() <-chan Event { return w.out }

// Run blocks until ctx is cancelled.
func (w *Watcher) Run(ctx context.Context) error {
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("inotify: %w", err)
	}
	defer func() { _ = fw.Close() }()

	if err := fw.Add(w.tasksDir); err != nil {
		return fmt.Errorf("watch %s: %w", w.tasksDir, err)
	}
	activePath := filepath.Join(w.tasksDir, "active")
	if _, err := os.Stat(activePath); err == nil {
		if err := fw.Add(activePath); err != nil {
			return fmt.Errorf("watch %s: %w", activePath, err)
		}
	}

	if err := w.scan(ctx, activePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("initial scan: %w", err)
	}

	debounce := time.NewTimer(time.Hour)
	debounce.Stop()
	pending := false

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err, ok := <-fw.Errors:
			if !ok {
				return nil
			}
			if err != nil {
				return fmt.Errorf("inotify error: %w", err)
			}
		case ev, ok := <-fw.Events:
			if !ok {
				return nil
			}
			if !relevantPath(ev.Name, activePath) {
				continue
			}
			if ev.Op&fsnotify.Create != 0 && ev.Name == activePath {
				_ = fw.Add(activePath)
			}
			if !pending {
				pending = true
				debounce.Reset(w.debounce)
			}
		case <-debounce.C:
			pending = false
			if err := w.scan(ctx, activePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("scan: %w", err)
			}
		}
	}
}

func relevantPath(p, activePath string) bool {
	return p == activePath || strings.HasSuffix(p, "/active")
}

func (w *Watcher) scan(ctx context.Context, activePath string) error {
	f, err := os.Open(activePath)
	if err != nil {
		return err
	}
	defer f.Close()
	return w.parseAll(ctx, f, time.Now())
}

func (w *Watcher) parseAll(ctx context.Context, r io.Reader, obs time.Time) error {
	seen := map[string]bool{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 16*1024), 512*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		upid, status, err := ParseActiveLine(line)
		if err != nil {
			continue
		}
		kind, action, ok := WorkerTypeMap(upid.Type)
		if !ok {
			continue
		}
		if !w.include[kind] {
			continue
		}
		seen[upid.Raw] = true
		w.mu.Lock()
		st := w.known[upid.Raw]
		emitStart := !st.startedEmitted
		emitEnd := status != "" && !st.finishedEmitted
		if emitStart {
			st.startedEmitted = true
		}
		if emitEnd {
			st.finishedEmitted = true
		}
		w.known[upid.Raw] = st
		w.mu.Unlock()

		if emitStart {
			w.out <- Event{UPID: upid, Kind: kind, Action: action, Phase: PhaseStarted, ObsTime: obs}
			if kind == events.KindQEMU && action == events.ActionMigrate {
				go w.watchMigrationSynced(ctx, upid)
			}
		}
		if emitEnd {
			phase := PhaseFinished
			if !isOK(status) {
				phase = PhaseFailed
			}
			w.out <- Event{
				UPID: upid, Kind: kind, Action: action,
				Phase: phase, Status: status, ObsTime: obs,
			}
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}

	// Garbage-collect UPIDs that disappeared from the active file and were already terminated.
	w.mu.Lock()
	for k, st := range w.known {
		if !seen[k] && st.finishedEmitted {
			delete(w.known, k)
		}
	}
	w.mu.Unlock()
	return nil
}

func isOK(status string) bool {
	return status == "OK" || strings.HasPrefix(status, "OK")
}

// migrationSyncedMarker is the line PVE appends to a live migration's task
// log once the target-side data (RAM state for online migrations) has fully
// caught up. It shows up roughly when "all 'mirror' jobs are ready" and the
// live migrate command hand off; PVE still has several seconds of cleanup
// (stopping the NBD server, flushing conntrack, removing the source volume)
// before the task's terminal status appears in the active file, so waiting
// for phase=finished/failed alone reports the event much later than useful.
const migrationSyncedMarker = "migration status: completed"

// watchMigrationSynced polls upid's per-task log file for migrationSyncedMarker
// and emits a single PhaseSynced event the first time it appears. It exits
// once the marker is found, the task's terminal event has already been
// emitted, or ctx is cancelled - whichever happens first.
func (w *Watcher) watchMigrationSynced(ctx context.Context, upid UPID) {
	ticker := time.NewTicker(w.syncPollInterval)
	defer ticker.Stop()

	var logFile string
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		w.mu.Lock()
		st, ok := w.known[upid.Raw]
		w.mu.Unlock()
		if !ok || st.finishedEmitted {
			return
		}

		if logFile == "" {
			f, err := FindLogFile(w.tasksDir, upid.Raw)
			if err != nil || f == "" {
				continue
			}
			logFile = f
		}

		b, err := os.ReadFile(logFile)
		if err != nil {
			continue
		}
		if !strings.Contains(string(b), migrationSyncedMarker) {
			continue
		}

		w.mu.Lock()
		st = w.known[upid.Raw]
		already := st.syncedEmitted
		st.syncedEmitted = true
		w.known[upid.Raw] = st
		w.mu.Unlock()
		if already {
			return
		}

		kind, action, ok := WorkerTypeMap(upid.Type)
		if !ok {
			return
		}
		w.out <- Event{UPID: upid, Kind: kind, Action: action, Phase: PhaseSynced, ObsTime: time.Now()}
		return
	}
}

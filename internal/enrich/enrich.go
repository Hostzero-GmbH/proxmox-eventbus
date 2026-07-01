// Package enrich converts a raw tasks.Event into a fully-populated events.CloudEvent
// by combining UPID metadata with pmxcfs configuration and (for migrations) the
// originating process's argv.
package enrich

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/events"
	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/pmxcfs"
	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/tasks"
)

type Enricher struct {
	Cluster string
	Node    string
	Reader  *pmxcfs.Reader

	// ProcRoot lets tests redirect /proc lookups.
	ProcRoot string
	// TasksRoot lets tests redirect /var/log/pve/tasks lookups.
	TasksRoot string
	// MigrateTargetWait bounds how long we wait for target on migrate.started
	// when immediate extraction fails.
	MigrateTargetWait time.Duration
	// MigrateTargetPoll controls retry interval while waiting for started target.
	MigrateTargetPoll time.Duration

	mu             sync.Mutex
	migrateTargets map[string]string
}

func (e *Enricher) procRoot() string {
	if e.ProcRoot == "" {
		return "/proc"
	}
	return e.ProcRoot
}

func (e *Enricher) tasksRoot() string {
	if e.TasksRoot == "" {
		return "/var/log/pve/tasks"
	}
	return e.TasksRoot
}

func (e *Enricher) migrateTargetWait() time.Duration {
	if e.MigrateTargetWait > 0 {
		return e.MigrateTargetWait
	}
	return 2 * time.Second
}

func (e *Enricher) migrateTargetPoll() time.Duration {
	if e.MigrateTargetPoll > 0 {
		return e.MigrateTargetPoll
	}
	return 100 * time.Millisecond
}

// Lifecycle turns a tasks.Event into a CloudEvent ready to publish.
func (e *Enricher) Lifecycle(in tasks.Event, taskStart time.Time) events.CloudEvent {
	vmid := in.UPID.VMID()
	data := events.EventData{
		Cluster:    e.Cluster,
		Node:       e.Node,
		Kind:       in.Kind,
		VMID:       vmid,
		Action:     in.Action,
		Phase:      events.Phase(in.Phase),
		UPID:       in.UPID.Raw,
		User:       in.UPID.User,
		SourceNode: e.Node,
	}

	if cfg, err := e.Reader.ReadVMConfig(pmxcfs.Kind(in.Kind), vmid); err == nil {
		data.Name = cfg.Name
		data.Tags = cfg.Tags
		data.Description = cfg.Description
		data.HookScript = cfg.HookScript
	}

	if in.Action == events.ActionMigrate {
		target := e.resolveMigrateTarget(in)
		if target == "" {
			target = e.recallMigrateTarget(in.UPID.Raw)
		}
		if target != "" {
			data.TargetNode = target
			if in.Phase == tasks.PhaseStarted {
				e.rememberMigrateTarget(in.UPID.Raw, target)
			}
		}
		if in.Phase == tasks.PhaseFinished || in.Phase == tasks.PhaseFailed {
			e.forgetMigrateTarget(in.UPID.Raw)
		}
	}

	if in.Phase == tasks.PhaseFinished || in.Phase == tasks.PhaseFailed {
		if !taskStart.IsZero() {
			d := time.Since(taskStart).Milliseconds()
			data.DurationMS = &d
		}
		data.ExitStatus = in.Status
	}

	return events.New(
		events.TypeFor(in.Kind, in.Action, events.Phase(in.Phase)),
		events.SourceFor(e.Cluster, e.Node),
		events.SubjectFor(in.Kind, vmid),
		data,
	)
}

func (e *Enricher) resolveMigrateTarget(in tasks.Event) string {
	if target := e.migrateTarget(in.UPID); target != "" {
		return target
	}
	if target := e.migrateTargetFromTaskLog(in.UPID); target != "" {
		return target
	}
	if in.Phase != tasks.PhaseStarted {
		return ""
	}

	deadline := time.Now().Add(e.migrateTargetWait())
	poll := e.migrateTargetPoll()
	for time.Now().Before(deadline) {
		time.Sleep(poll)
		if target := e.migrateTargetFromTaskLog(in.UPID); target != "" {
			return target
		}
	}
	return ""
}

// migrateTarget tries to extract the target node from the migration process's argv.
// Returns empty string if the process is gone or the argv format is unexpected.
//
// qm CLI:    qm migrate <vmid> <target> [--online] ...
// pct CLI:   pct migrate <vmid> <target> [--restart] ...
// HA agent:  the PID belongs to pve-ha-lrm; argv doesn't carry the target. In that
// case the watcher falls back to parsing the per-task log later (handled by caller).
func (e *Enricher) migrateTarget(upid tasks.UPID) string {
	cmdline, err := os.ReadFile(e.procRoot() + "/" + strconv.Itoa(upid.PID) + "/cmdline")
	if err != nil {
		return ""
	}
	return parseMigrateTarget(splitCmdline(cmdline))
}

func (e *Enricher) migrateTargetFromTaskLog(upid tasks.UPID) string {
	logFile, err := findTaskLogFile(e.tasksRoot(), upid.Raw)
	if err != nil || logFile == "" {
		return ""
	}
	b, err := os.ReadFile(logFile)
	if err != nil {
		return ""
	}
	return parseMigrateTargetFromLog(string(b))
}

var migrateTargetLogRe = regexp.MustCompile(`(?i)(?:target(?:[_ ]node)?\s*[=:]\s*|to\s+node\s+|to\s+)["']?([a-z0-9][a-z0-9._-]*)["']?`)

func parseMigrateTargetFromLog(s string) string {
	m := migrateTargetLogRe.FindStringSubmatch(s)
	if len(m) < 2 {
		return ""
	}
	if !looksLikeNode(m[1]) {
		return ""
	}
	return m[1]
}

func findTaskLogFile(root, upidRaw string) (string, error) {
	if root == "" || upidRaw == "" {
		return "", nil
	}
	name := strings.TrimSpace(upidRaw)
	if name == "" {
		return "", nil
	}
	trimmed := strings.TrimSuffix(name, ":")

	candidates := []string{
		filepath.Join(root, name),
		filepath.Join(root, trimmed),
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c, nil
		}
	}

	for _, n := range []string{name, trimmed} {
		matches, _ := filepath.Glob(filepath.Join(root, "*", n))
		for _, m := range matches {
			if fi, err := os.Stat(m); err == nil && !fi.IsDir() {
				return m, nil
			}
		}
	}

	const maxDepth = 3
	var found string
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return nil
		}
		if rel == "." {
			return nil
		}
		depth := strings.Count(rel, string(filepath.Separator))
		if d.IsDir() {
			if depth >= maxDepth {
				return fs.SkipDir
			}
			return nil
		}
		base := d.Name()
		if base == name || base == trimmed {
			found = path
			return fs.SkipAll
		}
		return nil
	})
	return found, nil
}

func parseMigrateTarget(args []string) string {
	if len(args) == 0 {
		return ""
	}

	if target := parseTargetOption(args); target != "" {
		return target
	}

	for i := 0; i < len(args); i++ {
		base := strings.ToLower(lastPathElement(args[i]))

		switch base {
		case "qm", "pct":
			if i+1 < len(args) && strings.EqualFold(args[i+1], "migrate") {
				if target := parseTargetAfterMigrate(args[i+2:]); target != "" {
					return target
				}
			}
		case "qmigrate", "vzmigrate":
			if target := parseTargetAfterMigrate(args[i+1:]); target != "" {
				return target
			}
		}
	}

	for i := 0; i < len(args); i++ {
		if strings.EqualFold(args[i], "migrate") {
			if target := parseTargetAfterMigrate(args[i+1:]); target != "" {
				return target
			}
		}
	}

	return ""
}

func parseTargetAfterMigrate(args []string) string {
	if target := parseTargetOption(args); target != "" {
		return target
	}

	seenVMID := false
	for _, arg := range args {
		if arg == "" || strings.HasPrefix(arg, "-") {
			continue
		}
		if k, v, ok := splitKV(arg); ok {
			switch strings.ToLower(k) {
			case "target", "targetnode", "target_node", "node":
				if looksLikeNode(v) {
					return v
				}
			case "vmid", "id":
				if _, err := strconv.Atoi(v); err == nil {
					seenVMID = true
				}
			}
			continue
		}
		if !seenVMID {
			if _, err := strconv.Atoi(arg); err == nil {
				seenVMID = true
			}
			continue
		}
		if looksLikeNode(arg) {
			return arg
		}
	}
	return ""
}

func parseTargetOption(args []string) string {
	for i := 0; i < len(args); i++ {
		arg := strings.TrimSpace(args[i])
		if arg == "" {
			continue
		}

		if k, v, ok := splitKV(arg); ok {
			switch strings.ToLower(k) {
			case "target", "targetnode", "target_node", "node", "to":
				if looksLikeNode(v) {
					return v
				}
			}
		}

		norm := strings.ToLower(arg)
		switch norm {
		case "--target", "-target", "--target-node", "--targetnode", "--to":
			if i+1 < len(args) && looksLikeNode(args[i+1]) {
				return args[i+1]
			}
		}
	}
	return ""
}

func splitKV(s string) (k, v string, ok bool) {
	key, val, found := strings.Cut(s, "=")
	if !found {
		return "", "", false
	}
	key = strings.TrimLeft(strings.TrimSpace(key), "-")
	val = strings.TrimSpace(val)
	if key == "" || val == "" {
		return "", "", false
	}
	return key, val, true
}

func looksLikeNode(s string) bool {
	if s == "" || strings.HasPrefix(s, "-") {
		return false
	}
	if _, err := strconv.Atoi(s); err == nil {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-', r == '_', r == '.':
		default:
			return false
		}
	}
	return true
}

func (e *Enricher) rememberMigrateTarget(upidRaw, target string) {
	if upidRaw == "" || target == "" {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.migrateTargets == nil {
		e.migrateTargets = map[string]string{}
	}
	e.migrateTargets[upidRaw] = target
}

func (e *Enricher) recallMigrateTarget(upidRaw string) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.migrateTargets == nil {
		return ""
	}
	return e.migrateTargets[upidRaw]
}

func (e *Enricher) forgetMigrateTarget(upidRaw string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.migrateTargets == nil {
		return
	}
	delete(e.migrateTargets, upidRaw)
}

func splitCmdline(b []byte) []string {
	parts := strings.Split(string(b), "\x00")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func lastPathElement(s string) string {
	if idx := strings.LastIndexByte(s, '/'); idx >= 0 {
		return s[idx+1:]
	}
	return s
}

var ErrNoConfig = errors.New("vm has no config in pmxcfs")

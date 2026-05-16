// Package enrich converts a raw tasks.Event into a fully-populated events.CloudEvent
// by combining UPID metadata with pmxcfs configuration and (for migrations) the
// originating process's argv.
package enrich

import (
	"errors"
	"os"
	"strconv"
	"strings"
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
}

func (e *Enricher) procRoot() string {
	if e.ProcRoot == "" {
		return "/proc"
	}
	return e.ProcRoot
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
		if target := e.migrateTarget(in.UPID); target != "" {
			data.TargetNode = target
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
	args := splitCmdline(cmdline)
	for i := 0; i+1 < len(args); i++ {
		base := strings.ToLower(lastPathElement(args[i]))
		if base != "qm" && base != "pct" {
			continue
		}
		if args[i+1] != "migrate" {
			continue
		}
		if i+3 < len(args) {
			return args[i+3]
		}
	}
	return ""
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

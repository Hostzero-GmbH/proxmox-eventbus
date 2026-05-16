// Package snapshot implements the periodic "general interrogation" emitter.
//
// Each tick reads /etc/pve/.vmlist (filtered to the local node) and probes the
// liveness of every owned VM/CT without touching the Proxmox HTTP API.
//
// QEMU: presence of /run/qemu-server/<vmid>.pid (and optional QMP query-status).
// LXC:  /sys/fs/cgroup/lxc/<vmid>/cgroup.procs non-empty.
package snapshot

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/events"
)

// Prober resolves running state for a single VM/CT.
type Prober struct {
	RunDir   string // default /run
	CgroupV2 string // default /sys/fs/cgroup
	ProcRoot string // default /proc
	UseQMP   bool
}

func (p Prober) runDir() string {
	if p.RunDir == "" {
		return "/run"
	}
	return p.RunDir
}

func (p Prober) cgroupRoot() string {
	if p.CgroupV2 == "" {
		return "/sys/fs/cgroup"
	}
	return p.CgroupV2
}

func (p Prober) procRoot() string {
	if p.ProcRoot == "" {
		return "/proc"
	}
	return p.ProcRoot
}

// QEMUState returns the running state for a QEMU VM.
//
// Cheap path: if /run/qemu-server/<vmid>.pid is missing the VM is stopped.
// If UseQMP is true and the .qmp socket exists, query-status distinguishes
// running/paused/suspended/io-error.
func (p Prober) QEMUState(vmid int) events.State {
	pidPath := filepath.Join(p.runDir(), "qemu-server", fmt.Sprintf("%d.pid", vmid))
	pid, err := readPID(pidPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return events.StateStopped
		}
		return events.StateUnknown
	}
	if !p.processAlive(pid) {
		return events.StateStopped
	}
	if !p.UseQMP {
		return events.StateRunning
	}
	qmp := filepath.Join(p.runDir(), "qemu-server", fmt.Sprintf("%d.qmp", vmid))
	if s, err := queryQMPStatus(qmp); err == nil {
		return mapQMPStatus(s)
	}
	return events.StateRunning
}

// LXCState returns the running state for an LXC container.
func (p Prober) LXCState(vmid int) events.State {
	procs := filepath.Join(p.cgroupRoot(), "lxc", fmt.Sprintf("%d", vmid), "cgroup.procs")
	b, err := os.ReadFile(procs)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return events.StateStopped
		}
		return events.StateUnknown
	}
	if strings.TrimSpace(string(b)) == "" {
		return events.StateStopped
	}
	return events.StateRunning
}

func readPID(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	if !sc.Scan() {
		return 0, errors.New("empty pid file")
	}
	var pid int
	if _, err := fmt.Sscanf(strings.TrimSpace(sc.Text()), "%d", &pid); err != nil {
		return 0, err
	}
	return pid, nil
}

func (p Prober) processAlive(pid int) bool {
	if pid <= 1 {
		return false
	}
	// When /proc isn't available (non-Linux dev hosts, tests), trust the pid file.
	if _, err := os.Stat(p.procRoot()); err != nil {
		return true
	}
	_, err := os.Stat(fmt.Sprintf("%s/%d", p.procRoot(), pid))
	return err == nil
}

func mapQMPStatus(s string) events.State {
	switch s {
	case "running":
		return events.StateRunning
	case "paused", "prelaunch":
		return events.StatePaused
	case "suspended":
		return events.StateSuspended
	case "io-error", "guest-panicked", "internal-error", "watchdog":
		return events.StateUnknown
	default:
		return events.StateUnknown
	}
}

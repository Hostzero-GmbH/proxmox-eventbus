// Package snapshot implements the periodic "general interrogation" emitter.
//
// Each tick reads /etc/pve/.vmlist (filtered to the local node) and probes the
// liveness of every owned VM/CT without touching the Proxmox HTTP API.
//
// QEMU: presence of /run/qemu-server/<vmid>.pid is authoritative for the
// running state - PVE removes the file on graceful stop. If the daemon has
// permission to read the file (root, or CAP_DAC_READ_SEARCH) we also do an
// active kill(pid,0) liveness check to catch stale post-crash files; without
// that permission we trust the file's existence.
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
	"syscall"

	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/events"
)

// Prober resolves running state for a single VM/CT.
type Prober struct {
	RunDir   string // default /run
	CgroupV2 string // default /sys/fs/cgroup
	ProcRoot string // default /proc; only used by tests, see processAlive
	UseQMP   bool
}

// errStalePID is returned by readPID when the file exists but is empty or
// unparseable. PVE writes the PID atomically on start so an empty/garbage
// file means a crashed/killed startup; we treat it as Stopped.
var errStalePID = errors.New("stale pid file")

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

// QEMUState returns the running state and a diagnostic detail for a QEMU VM.
// The detail string is empty on the happy paths and only populated when state
// is StateUnknown to make the failure debuggable from the event payload.
func (p Prober) QEMUState(vmid int) (events.State, string) {
	pidPath := filepath.Join(p.runDir(), "qemu-server", fmt.Sprintf("%d.pid", vmid))

	if _, err := os.Stat(pidPath); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return events.StateStopped, ""
		}
		return events.StateUnknown, fmt.Sprintf("stat %s: %v", pidPath, err)
	}

	pid, err := readPID(pidPath)
	if err != nil {
		// The file exists. We need to distinguish:
		//   - empty/garbage -> Stopped (crash before PVE wrote the pid)
		//   - permission denied -> trust existence (PVE creates mode 0600
		//     root:root and our daemon is unprivileged in the default unit)
		if errors.Is(err, errStalePID) {
			return events.StateStopped, ""
		}
		return events.StateRunning, ""
	}

	if !p.processAlive(pid) {
		return events.StateStopped, ""
	}
	if !p.UseQMP {
		return events.StateRunning, ""
	}
	qmp := filepath.Join(p.runDir(), "qemu-server", fmt.Sprintf("%d.qmp", vmid))
	if s, err := queryQMPStatus(qmp); err == nil {
		return mapQMPStatus(s), ""
	}
	return events.StateRunning, ""
}

// LXCState returns the running state for an LXC container.
//
// On cgroup v2 PVE places container processes in either
// /sys/fs/cgroup/lxc/<vmid>/cgroup.procs (older) or
// /sys/fs/cgroup/lxc.payload-<vmid>/cgroup.procs (systemd slice). We check
// both layouts before declaring Stopped.
func (p Prober) LXCState(vmid int) (events.State, string) {
	candidates := []string{
		filepath.Join(p.cgroupRoot(), "lxc", fmt.Sprintf("%d", vmid), "cgroup.procs"),
		filepath.Join(p.cgroupRoot(), fmt.Sprintf("lxc.payload-%d", vmid), "cgroup.procs"),
		filepath.Join(p.cgroupRoot(), fmt.Sprintf("lxc.payload.%d", vmid), "cgroup.procs"),
	}
	for _, path := range candidates {
		b, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			// Other I/O error on a path that should exist.
			return events.StateUnknown, fmt.Sprintf("read %s: %v", path, err)
		}
		if strings.TrimSpace(string(b)) == "" {
			return events.StateStopped, ""
		}
		return events.StateRunning, ""
	}
	return events.StateStopped, ""
}

func readPID(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	if !sc.Scan() {
		return 0, errStalePID
	}
	var pid int
	if _, err := fmt.Sscanf(strings.TrimSpace(sc.Text()), "%d", &pid); err != nil {
		return 0, errStalePID
	}
	return pid, nil
}

// processAlive uses kill(pid, 0). This bypasses /proc visibility restrictions
// imposed by systemd's ProtectProc=invisible, which would otherwise hide
// processes owned by other users from our non-root daemon.
//
// ProcRoot is honoured only when explicitly set (in tests) so the existing
// fake-/proc test setup keeps working.
func (p Prober) processAlive(pid int) bool {
	if pid <= 1 {
		return false
	}
	if p.ProcRoot != "" {
		_, err := os.Stat(fmt.Sprintf("%s/%d", p.ProcRoot, pid))
		return err == nil
	}
	err := syscall.Kill(pid, 0)
	if err == nil {
		return true
	}
	// EPERM means the process exists but we may not signal it.
	return errors.Is(err, syscall.EPERM)
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

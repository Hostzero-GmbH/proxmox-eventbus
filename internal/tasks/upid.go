// Package tasks parses Proxmox UPIDs and watches the task log via inotify.
package tasks

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// UPID is a Proxmox Unique Process ID.
//
//	UPID:<node>:<pid_hex>:<pstart_hex>:<starttime_hex>:<type>:<id>:<user>:
type UPID struct {
	Node      string
	PID       int
	PStart    uint64
	StartTime int64
	Type      string
	ID        string
	User      string
	Raw       string
}

// ParseUPID parses a UPID with the strict 8-segment layout used by PVE since 7.x.
func ParseUPID(s string) (UPID, error) {
	if !strings.HasPrefix(s, "UPID:") {
		return UPID{}, fmt.Errorf("missing UPID prefix: %q", s)
	}
	body := s[len("UPID:"):]
	body = strings.TrimSuffix(body, ":")
	parts := strings.Split(body, ":")
	if len(parts) < 7 {
		return UPID{}, fmt.Errorf("UPID has %d fields, want >=7: %q", len(parts), s)
	}
	pid, err := strconv.ParseInt(parts[1], 16, 32)
	if err != nil {
		return UPID{}, fmt.Errorf("pid: %w", err)
	}
	pstart, err := strconv.ParseUint(parts[2], 16, 64)
	if err != nil {
		return UPID{}, fmt.Errorf("pstart: %w", err)
	}
	st, err := strconv.ParseInt(parts[3], 16, 64)
	if err != nil {
		return UPID{}, fmt.Errorf("starttime: %w", err)
	}
	return UPID{
		Node:      parts[0],
		PID:       int(pid),
		PStart:    pstart,
		StartTime: st,
		Type:      parts[4],
		ID:        parts[5],
		User:      parts[6],
		Raw:       s,
	}, nil
}

// IsLifecycle reports whether the worker type maps to a kind+action we publish.
func (u UPID) IsLifecycle() bool {
	_, _, ok := WorkerTypeMap(u.Type)
	return ok
}

// VMID returns the VMID encoded in the ID field, or -1 if not present.
func (u UPID) VMID() int {
	if u.ID == "" {
		return -1
	}
	id, err := strconv.Atoi(u.ID)
	if err != nil {
		return -1
	}
	return id
}

var errEmpty = errors.New("empty line")

// ParseActiveLine parses a single line from /var/log/pve/tasks/active or
// /var/log/pve/tasks/index. PVE writes both as space-separated records.
//
//	<UPID>                            (legacy active line, no trailer)
//	<UPID> 0                          (active, not yet saved to index)
//	<UPID> 1 <endtime_hex> <status>   (active, finished and saved)
//	<UPID> <endtime_hex> <status>     (index file form)
//
// Returns the canonical UPID (with trailing colon, no trailer) and the status
// string ("" while running, "OK" or an error message after termination).
func ParseActiveLine(line string) (UPID, string, error) {
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return UPID{}, "", errEmpty
	}
	upidStr, rest, _ := strings.Cut(line, " ")
	upid, err := ParseUPID(upidStr)
	if err != nil {
		return UPID{}, "", err
	}
	return upid, parseStatusTail(rest), nil
}

func parseStatusTail(rest string) string {
	rest = strings.TrimSpace(rest)
	if rest == "" || rest == "0" {
		return ""
	}
	fields := strings.Fields(rest)
	// Active "1 <endtime_hex> <status...>"
	if fields[0] == "1" && len(fields) >= 3 && isHex(fields[1]) {
		return strings.Join(fields[2:], " ")
	}
	// Index "<endtime_hex> <status...>"
	if len(fields) >= 2 && isHex(fields[0]) {
		return strings.Join(fields[1:], " ")
	}
	return rest
}

func isHex(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		switch {
		case '0' <= c && c <= '9':
		case 'a' <= c && c <= 'f':
		case 'A' <= c && c <= 'F':
		default:
			return false
		}
	}
	return true
}

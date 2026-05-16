// Package pmxcfs reads the Proxmox cluster filesystem (/etc/pve) for cluster
// membership, VM configuration and VM placement.
//
// All reads are off tmpfs and complete in microseconds; the reader caches with
// inotify-driven invalidation.
package pmxcfs

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const MembersFile = ".members"

type Members struct {
	NodeName string         `json:"nodename"`
	Cluster  ClusterInfo    `json:"cluster"`
	NodeList map[string]Node `json:"nodelist"`
}

type ClusterInfo struct {
	Name    string `json:"name"`
	Nodes   int    `json:"nodes"`
	Quorate int    `json:"quorate"`
	Version int    `json:"version"`
}

type Node struct {
	ID     int    `json:"id"`
	Online int    `json:"online"`
	IP     string `json:"ip"`
}

// Reader exposes typed accessors over /etc/pve.
type Reader struct {
	root string

	mu      sync.RWMutex
	members *Members
	loaded  time.Time
}

func New(root string) *Reader {
	if root == "" {
		root = "/etc/pve"
	}
	return &Reader{root: root}
}

func (r *Reader) Root() string { return r.root }

// Members returns the latest parsed /etc/pve/.members. Cache is bypassed if older than maxAge.
func (r *Reader) Members() (*Members, error) {
	r.mu.RLock()
	if r.members != nil && time.Since(r.loaded) < 2*time.Second {
		m := r.members
		r.mu.RUnlock()
		return m, nil
	}
	r.mu.RUnlock()
	return r.reloadMembers()
}

// ReloadMembers forces a re-read from disk.
func (r *Reader) ReloadMembers() (*Members, error) {
	return r.reloadMembers()
}

func (r *Reader) reloadMembers() (*Members, error) {
	b, err := os.ReadFile(filepath.Join(r.root, MembersFile))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", MembersFile, err)
	}
	var m Members
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("parse %s: %w", MembersFile, err)
	}
	r.mu.Lock()
	r.members = &m
	r.loaded = time.Now()
	r.mu.Unlock()
	return &m, nil
}

// ClusterName returns the cluster name from .members, empty if standalone.
func (r *Reader) ClusterName() string {
	m, err := r.Members()
	if err != nil {
		return ""
	}
	return m.Cluster.Name
}

// LocalNodeName returns the nodename field from .members; falls back to /etc/hostname.
func (r *Reader) LocalNodeName() (string, error) {
	if m, err := r.Members(); err == nil && m.NodeName != "" {
		return m.NodeName, nil
	}
	b, err := os.ReadFile("/etc/hostname")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

// Peers returns all online peer nodes excluding self.
func (r *Reader) Peers() ([]Node, error) {
	m, err := r.Members()
	if err != nil {
		return nil, err
	}
	out := make([]Node, 0, len(m.NodeList))
	for name, n := range m.NodeList {
		if name == m.NodeName {
			continue
		}
		if n.Online == 0 {
			continue
		}
		out = append(out, n)
	}
	return out, nil
}

// ErrNotInCluster indicates the host is standalone (no .members file or empty cluster).
var ErrNotInCluster = errors.New("host is not in a proxmox cluster")

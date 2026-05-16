package pmxcfs

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Kind string

const (
	KindQEMU Kind = "qemu"
	KindLXC  Kind = "lxc"
)

// VMConfig is a minimal projection of a PVE VM/CT configuration file.
type VMConfig struct {
	Kind        Kind
	VMID        int
	Name        string
	Tags        []string
	Description string
	HookScript  string
	// Net holds raw netN= lines keyed by index for downstream consumers that want MAC/VLAN context.
	Net map[string]string
	// Raw retains every top-level key for advanced consumers.
	Raw map[string]string
}

// ReadVMConfig reads a qemu-server or lxc config file.
//
// PVE config files contain a default section followed by [snapshot]/[pending] sections.
// We only parse the default section; everything beneath the first [foo] header is ignored.
func (r *Reader) ReadVMConfig(kind Kind, vmid int) (*VMConfig, error) {
	var path string
	switch kind {
	case KindQEMU:
		path = filepath.Join(r.root, "qemu-server", fmt.Sprintf("%d.conf", vmid))
	case KindLXC:
		path = filepath.Join(r.root, "lxc", fmt.Sprintf("%d.conf", vmid))
	default:
		return nil, fmt.Errorf("unknown kind %q", kind)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	cfg := &VMConfig{Kind: kind, VMID: vmid, Net: map[string]string{}, Raw: map[string]string{}}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimRight(sc.Text(), "\r\n")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[") {
			break
		}
		idx := strings.IndexByte(line, ':')
		if idx <= 0 {
			continue
		}
		k := strings.TrimSpace(line[:idx])
		v := strings.TrimSpace(line[idx+1:])
		cfg.Raw[k] = v
		switch {
		case k == "name", k == "hostname":
			cfg.Name = v
		case k == "tags":
			cfg.Tags = splitTags(v)
		case k == "description":
			cfg.Description = unescapeDescription(v)
		case k == "hookscript":
			cfg.HookScript = v
		case strings.HasPrefix(k, "net"):
			cfg.Net[k] = v
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func splitTags(v string) []string {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil
	}
	parts := strings.FieldsFunc(v, func(r rune) bool {
		return r == ';' || r == ',' || r == ' '
	})
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// PVE encodes newlines as literal "\n" inside the description field.
func unescapeDescription(v string) string {
	return strings.ReplaceAll(v, `\n`, "\n")
}

// VMList enumerates VMIDs owned by the local node by listing config files.
func (r *Reader) VMList() (qemu, lxc []int, err error) {
	q, err := listConfigDir(filepath.Join(r.root, "qemu-server"))
	if err != nil {
		return nil, nil, err
	}
	l, err := listConfigDir(filepath.Join(r.root, "lxc"))
	if err != nil {
		return nil, nil, err
	}
	return q, l, nil
}

func listConfigDir(dir string) ([]int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("readdir %s: %w", dir, err)
	}
	out := make([]int, 0, len(entries))
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".conf") {
			continue
		}
		idStr := strings.TrimSuffix(name, ".conf")
		id, err := strconv.Atoi(idStr)
		if err != nil {
			continue
		}
		out = append(out, id)
	}
	return out, nil
}

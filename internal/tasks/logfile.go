package tasks

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// FindLogFile locates the per-task log file for upidRaw under root (typically
// /var/log/pve/tasks). PVE shards logs into subdirectories; we try the
// obvious direct/glob paths first and fall back to a bounded directory walk
// for layouts we don't recognise.
func FindLogFile(root, upidRaw string) (string, error) {
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

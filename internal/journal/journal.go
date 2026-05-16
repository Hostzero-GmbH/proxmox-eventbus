// Package journal provides slog handlers tuned for systemd's journald capture.
//
// We avoid the native sd_journal socket and the go-systemd CGO journal binding;
// instead we write to stderr with severity prefixes ("<3>" for error, "<6>" for
// info, etc.) which systemd's journald translates into priority fields when the
// service is run under systemd. The result is structured journal entries with
// zero extra dependencies.
package journal

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

// NewHandler returns a slog.Handler appropriate for the requested format.
//
// formats: "journald" (default; severity-prefixed text to stderr),
//
//	"json" (slog.NewJSONHandler), "text" (slog.NewTextHandler).
func NewHandler(format, level string) slog.Handler {
	lvl := parseLevel(level)
	switch format {
	case "json":
		return slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})
	case "text":
		return slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})
	default:
		return &journaldHandler{w: os.Stderr, lvl: lvl}
	}
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

type journaldHandler struct {
	w     io.Writer
	lvl   slog.Level
	attrs []slog.Attr
	group string
}

func (h *journaldHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.lvl }

func (h *journaldHandler) Handle(_ context.Context, r slog.Record) error {
	prio := sdPriority(r.Level)
	sb := &strings.Builder{}
	fmt.Fprintf(sb, "<%d>%s", prio, r.Message)
	for _, a := range h.attrs {
		writeAttr(sb, h.group, a)
	}
	r.Attrs(func(a slog.Attr) bool {
		writeAttr(sb, h.group, a)
		return true
	})
	sb.WriteByte('\n')
	_, err := io.WriteString(h.w, sb.String())
	return err
}

func (h *journaldHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	out := *h
	out.attrs = append(append([]slog.Attr(nil), h.attrs...), attrs...)
	return &out
}

func (h *journaldHandler) WithGroup(name string) slog.Handler {
	out := *h
	out.group = name
	return &out
}

func sdPriority(l slog.Level) int {
	switch {
	case l <= slog.LevelDebug:
		return 7
	case l <= slog.LevelInfo:
		return 6
	case l <= slog.LevelWarn:
		return 4
	default:
		return 3
	}
}

func writeAttr(sb *strings.Builder, group string, a slog.Attr) {
	key := a.Key
	if group != "" {
		key = group + "." + key
	}
	v := a.Value.Resolve()
	switch v.Kind() {
	case slog.KindString:
		fmt.Fprintf(sb, " %s=%q", key, v.String())
	default:
		fmt.Fprintf(sb, " %s=%v", key, v.Any())
	}
}

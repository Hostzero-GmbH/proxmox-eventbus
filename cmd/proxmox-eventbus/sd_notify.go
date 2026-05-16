package main

import (
	"net"
	"os"
)

// notifyReady sends READY=1 to systemd via $NOTIFY_SOCKET when the service is
// run under Type=notify. Silently no-ops when the env var is unset.
func notifyReady() {
	sock := os.Getenv("NOTIFY_SOCKET")
	if sock == "" {
		return
	}
	addr := &net.UnixAddr{Name: sock, Net: "unixgram"}
	c, err := net.DialUnix("unixgram", nil, addr)
	if err != nil {
		return
	}
	defer c.Close()
	_, _ = c.Write([]byte("READY=1\nSTATUS=running\n"))
}

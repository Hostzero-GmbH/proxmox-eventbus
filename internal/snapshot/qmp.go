package snapshot

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"time"
)

// queryQMPStatus opens a QMP socket, performs the capabilities handshake, runs
// `query-status`, and returns the status string.
//
// QEMU's QMP socket is a unix domain socket; the protocol speaks JSON lines.
func queryQMPStatus(sock string) (string, error) {
	c, err := net.DialTimeout("unix", sock, 200*time.Millisecond)
	if err != nil {
		return "", err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(500 * time.Millisecond))

	r := bufio.NewReader(c)
	if _, err := readJSON(r); err != nil {
		return "", fmt.Errorf("greeting: %w", err)
	}

	if err := writeJSON(c, map[string]any{"execute": "qmp_capabilities"}); err != nil {
		return "", err
	}
	if _, err := readJSON(r); err != nil {
		return "", fmt.Errorf("capabilities ack: %w", err)
	}

	if err := writeJSON(c, map[string]any{"execute": "query-status"}); err != nil {
		return "", err
	}
	resp, err := readJSON(r)
	if err != nil {
		return "", fmt.Errorf("query-status: %w", err)
	}
	ret, ok := resp["return"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("query-status: no return: %v", resp)
	}
	status, _ := ret["status"].(string)
	return status, nil
}

func writeJSON(c net.Conn, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b = append(b, '\r', '\n')
	_, err = c.Write(b)
	return err
}

func readJSON(r *bufio.Reader) (map[string]any, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(line, &m); err != nil {
		return nil, err
	}
	return m, nil
}

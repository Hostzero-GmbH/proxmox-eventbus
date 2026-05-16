// Package tlsutil loads PVE-issued certificates and rebuilds tls.Config atomically.
//
// Both the NATS client listener and the cluster route listener reuse the cluster CA
// at /etc/pve/pve-root-ca.pem and the per-node server certificate at
// /etc/pve/local/pve-ssl.{pem,key}. The loader watches these paths so that
// `pvecm updatecerts` is picked up without restarting the daemon.
package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

type Loader struct {
	caFile, certFile, keyFile string
	verifyClient              bool

	cur     atomic.Pointer[tls.Config]
	caCur   atomic.Pointer[x509.CertPool]
	lastMod time.Time
}

func NewLoader(ca, cert, key string, verifyClient bool) (*Loader, error) {
	l := &Loader{caFile: ca, certFile: cert, keyFile: key, verifyClient: verifyClient}
	if err := l.Reload(); err != nil {
		return nil, err
	}
	return l, nil
}

// Reload reads the files from disk and atomically swaps the current tls.Config.
func (l *Loader) Reload() error {
	caPEM, err := os.ReadFile(l.caFile)
	if err != nil {
		return fmt.Errorf("read CA %s: %w", l.caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return fmt.Errorf("no PEM blocks in %s", l.caFile)
	}
	cert, err := tls.LoadX509KeyPair(l.certFile, l.keyFile)
	if err != nil {
		return fmt.Errorf("load keypair %s/%s: %w", l.certFile, l.keyFile, err)
	}
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ClientCAs:    pool,
	}
	if l.verifyClient {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	l.cur.Store(cfg)
	l.caCur.Store(pool)
	return nil
}

// Server returns a tls.Config wired to dynamically reload the current cert/pool.
func (l *Loader) Server() *tls.Config {
	base := l.cur.Load()
	cfg := base.Clone()
	cfg.GetConfigForClient = func(_ *tls.ClientHelloInfo) (*tls.Config, error) {
		return l.cur.Load(), nil
	}
	cfg.GetCertificate = func(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
		c := l.cur.Load()
		return &c.Certificates[0], nil
	}
	return cfg
}

// Client returns a tls.Config for outbound mTLS connections.
func (l *Loader) Client(serverName string) *tls.Config {
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: serverName,
		RootCAs:    l.caCur.Load(),
	}
	cfg.GetClientCertificate = func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
		c := l.cur.Load()
		return &c.Certificates[0], nil
	}
	return cfg
}

// Paths returns the loader file paths so the caller can wire inotify.
func (l *Loader) Paths() []string {
	return []string{l.caFile, l.certFile, l.keyFile}
}

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
	"errors"
	"fmt"
	"os"
	"sync/atomic"
)

type Loader struct {
	caFile, certFile, keyFile string
	verifyClient              bool

	cur   atomic.Pointer[tls.Config]
	caCur atomic.Pointer[x509.CertPool]
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

// Cluster returns a tls.Config for the NATS cluster route listener (port 6222).
//
// PVE's per-node pve-ssl.pem is issued for HTTPS, so it has SAN=management-IP
// only and EKU=serverAuth only. Go's default TLS verification rejects this
// when the cert is reached over the cluster-network IP (from /etc/pve/.members)
// or used in client-role mTLS (Go expects ExtKeyUsageClientAuth on the leaf).
//
// The cluster listener therefore:
//   - InsecureSkipVerify on the client (outbound) role - skips hostname check.
//   - ClientAuth=RequireAnyClientCert on the server (inbound) role - keeps
//     Go from running its own chain check with KeyUsages=ClientAuth.
//   - VerifyPeerCertificate on both - we run chain verification ourselves
//     against the PVE cluster CA, ignoring SAN and EKU.
//
// GetConfigForClient is overridden so that the per-handshake config picked up
// by the TLS server on every inbound peer connection still carries these
// cluster-specific settings; without that override Go would fall back to the
// stricter base config and reject every PVE-issued client cert with
// "x509: certificate specifies an incompatible key usage".
//
// Trust boundary: any cert signed by /etc/pve/pve-root-ca.pem is a valid
// cluster peer, regardless of the IP or EKU it presents. This matches PVE's
// existing PKI trust model (the CA key lives root-only in pmxcfs).
func (l *Loader) Cluster() *tls.Config {
	cfg := l.Server()
	l.applyClusterSettings(cfg)
	cfg.GetConfigForClient = func(_ *tls.ClientHelloInfo) (*tls.Config, error) {
		c := l.cur.Load().Clone()
		l.applyClusterSettings(c)
		return c, nil
	}
	return cfg
}

func (l *Loader) applyClusterSettings(cfg *tls.Config) {
	cfg.InsecureSkipVerify = true
	cfg.ClientAuth = tls.RequireAnyClientCert
	cfg.VerifyPeerCertificate = l.verifyChain
}

// verifyChain validates a peer's certificate chain against the current
// PVE cluster CA pool. Used as VerifyPeerCertificate when default Go
// verification is disabled for both client and server roles on cluster routes.
//
// EKU is deliberately ignored: PVE leaves EKU=serverAuth on per-node certs
// even though we use them in both server and client roles here.
func (l *Loader) verifyChain(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	if len(rawCerts) == 0 {
		return errors.New("no peer certificate")
	}
	leaf, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return fmt.Errorf("parse leaf: %w", err)
	}
	// Clearing EKU before Verify makes checkChainForKeyUsage accept the cert
	// for any role. Equivalent to passing KeyUsages=[ExtKeyUsageAny] on
	// modern Go but works the same way across stdlib versions.
	leaf.ExtKeyUsage = nil
	leaf.UnknownExtKeyUsage = nil
	intermediates := x509.NewCertPool()
	for _, raw := range rawCerts[1:] {
		c, err := x509.ParseCertificate(raw)
		if err != nil {
			continue
		}
		c.ExtKeyUsage = nil
		c.UnknownExtKeyUsage = nil
		intermediates.AddCert(c)
	}
	_, err = leaf.Verify(x509.VerifyOptions{
		Roots:         l.caCur.Load(),
		Intermediates: intermediates,
	})
	return err
}

// Paths returns the loader file paths so the caller can wire inotify.
func (l *Loader) Paths() []string {
	return []string{l.caFile, l.certFile, l.keyFile}
}

package bus

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/tlsutil"
	nats "github.com/nats-io/nats.go"
)

type testLogger struct{ t *testing.T }

func (l testLogger) Debug(msg string, args ...any) { l.t.Logf("DBG "+msg, args...) }
func (l testLogger) Info(msg string, args ...any)  { l.t.Logf("INF "+msg, args...) }
func (l testLogger) Warn(msg string, args ...any)  { l.t.Logf("WRN "+msg, args...) }
func (l testLogger) Error(msg string, args ...any) { l.t.Logf("ERR "+msg, args...) }

var _ = log.Print

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func newCA(t *testing.T, dir string) (caPath, keyPath string, caCert *x509.Certificate, caKey *ecdsa.PrivateKey) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-ca"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ = x509.ParseCertificate(der)
	caPath = filepath.Join(dir, "ca.pem")
	keyPath = filepath.Join(dir, "ca.key")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		t.Fatal(err)
	}
	kb, _ := x509.MarshalPKCS8PrivateKey(caKey)
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kb}), 0o600); err != nil {
		t.Fatal(err)
	}
	return
}

func signNodeCert(t *testing.T, dir, name string, caCert *x509.Certificate, caKey *ecdsa.PrivateKey) (certPath, keyPath string) {
	t.Helper()
	priv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost", name},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &priv.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	certPath = filepath.Join(dir, name+".pem")
	keyPath = filepath.Join(dir, name+".key")
	_ = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644)
	kb, _ := x509.MarshalPKCS8PrivateKey(priv)
	_ = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kb}), 0o600)
	return
}

func TestTwoNodeRouting(t *testing.T) {
	dir := t.TempDir()
	caPath, _, caCert, caKey := newCA(t, dir)

	n1Cert, n1Key := signNodeCert(t, dir, "node1", caCert, caKey)
	n2Cert, n2Key := signNodeCert(t, dir, "node2", caCert, caKey)

	l1, err := tlsutil.NewLoader(caPath, n1Cert, n1Key, false)
	if err != nil {
		t.Fatal(err)
	}
	l2, err := tlsutil.NewLoader(caPath, n2Cert, n2Key, false)
	if err != nil {
		t.Fatal(err)
	}

	clientPort1 := freePort(t)
	clusterPort1 := freePort(t)
	clientPort2 := freePort(t)
	clusterPort2 := freePort(t)

	tl := testLogger{t}
	s1 := New(Options{
		ServerName:   "node1",
		ClientHost:   "127.0.0.1",
		ClientPort:   clientPort1,
		ClusterHost:  "127.0.0.1",
		ClusterPort:  clusterPort1,
		TLS:          l1.Server(),
		ClusterTLS:   l1.Cluster(),
		StaticRoutes: []string{fmt.Sprintf("nats-route://127.0.0.1:%d", clusterPort2)},
		Logger:       tl,
	})
	s2 := New(Options{
		ServerName:   "node2",
		ClientHost:   "127.0.0.1",
		ClientPort:   clientPort2,
		ClusterHost:  "127.0.0.1",
		ClusterPort:  clusterPort2,
		TLS:          l2.Server(),
		ClusterTLS:   l2.Cluster(),
		StaticRoutes: []string{fmt.Sprintf("nats-route://127.0.0.1:%d", clusterPort1)},
		Logger:       tl,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s1.Start(ctx); err != nil {
		t.Fatalf("s1 start: %v", err)
	}
	defer s1.Stop()
	if err := s2.Start(ctx); err != nil {
		t.Fatalf("s2 start: %v", err)
	}
	defer s2.Stop()

	// Wait briefly for cluster routes to establish.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s1.Conn().NumSubscriptions() >= 0 && s2.Conn().NumSubscriptions() >= 0 &&
			countRoutes(t, s1) >= 1 && countRoutes(t, s2) >= 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Subscribe on s2; need to wait for subscription interest to propagate to s1 via the route.
	got := make(chan string, 1)
	sub, err := s2.Conn().Subscribe("pve.demo.*.qemu.>", func(m *nats.Msg) {
		got <- string(m.Data)
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()
	if err := s2.Conn().Flush(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)

	// Publish from s1; retry up to a few times in case interest is slow to gossip.
	deadline2 := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline2) {
		_ = s1.Conn().Publish("pve.demo.pve1.qemu.101.start.started", []byte(`{"hello":"world"}`))
		_ = s1.Conn().Flush()
		select {
		case msg := <-got:
			if msg != `{"hello":"world"}` {
				t.Errorf("payload = %q", msg)
			}
			_ = tls.VersionTLS12
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
	t.Fatal("did not receive routed message")
}

func countRoutes(t *testing.T, s *Server) int {
	t.Helper()
	if s.ns == nil {
		return 0
	}
	return s.ns.NumRoutes()
}

// TestServerAdvertise verifies that ClientAdvertise/ClusterAdvertise from
// bus.Options flow through to the underlying natsserver options.
func TestServerAdvertise(t *testing.T) {
	dir := t.TempDir()
	caPath, _, caCert, caKey := newCA(t, dir)
	nCert, nKey := signNodeCert(t, dir, "node1", caCert, caKey)
	loader, err := tlsutil.NewLoader(caPath, nCert, nKey, false)
	if err != nil {
		t.Fatal(err)
	}

	s := New(Options{
		ServerName:       "node1",
		ClientHost:       "127.0.0.1",
		ClientPort:       freePort(t),
		ClientAdvertise:  "10.99.99.99:4222",
		ClusterHost:      "127.0.0.1",
		ClusterPort:      freePort(t),
		ClusterAdvertise: "10.99.99.99:6222",
		TLS:              loader.Server(),
		ClusterTLS:       loader.Cluster(),
		Logger:           testLogger{t},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()
	if got := s.natsOpts.ClientAdvertise; got != "10.99.99.99:4222" {
		t.Errorf("ClientAdvertise = %q, want 10.99.99.99:4222", got)
	}
	if got := s.natsOpts.Cluster.Advertise; got != "10.99.99.99:6222" {
		t.Errorf("Cluster.Advertise = %q, want 10.99.99.99:6222", got)
	}
}

// signPVEStyleNodeCert mints a cert that mimics PVE's per-node pve-ssl.pem:
//   - SAN contains only the management hostname (not the dialed cluster IP).
//   - EKU is serverAuth only (no clientAuth).
//
// This is the exact shape that broke Cluster() in production: Go's TLS server
// rejected the inbound client cert with "incompatible key usage" because it
// has no clientAuth EKU.
func signPVEStyleNodeCert(t *testing.T, dir, name string, caCert *x509.Certificate, caKey *ecdsa.PrivateKey) (certPath, keyPath string) {
	t.Helper()
	priv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name + ".nov.hostzero.com"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{name + ".nov.hostzero.com", name},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &priv.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	certPath = filepath.Join(dir, name+".pem")
	keyPath = filepath.Join(dir, name+".key")
	_ = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644)
	kb, _ := x509.MarshalPKCS8PrivateKey(priv)
	_ = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kb}), 0o600)
	return
}

// TestClusterRoutingPVEStyleCerts reproduces the production setup exactly:
//   - certs are signed by the cluster CA with EKU=serverAuth and SAN=hostname,
//   - peers reach each other via a cluster-network IP not in the SAN.
//
// Without proper GetConfigForClient handling Go's TLS server would reject the
// inbound cert with "x509: certificate specifies an incompatible key usage".
func TestClusterRoutingPVEStyleCerts(t *testing.T) {
	dir := t.TempDir()
	caPath, _, caCert, caKey := newCA(t, dir)

	n1Cert, n1Key := signPVEStyleNodeCert(t, dir, "pve01", caCert, caKey)
	n2Cert, n2Key := signPVEStyleNodeCert(t, dir, "pve02", caCert, caKey)

	l1, err := tlsutil.NewLoader(caPath, n1Cert, n1Key, false)
	if err != nil {
		t.Fatal(err)
	}
	l2, err := tlsutil.NewLoader(caPath, n2Cert, n2Key, false)
	if err != nil {
		t.Fatal(err)
	}

	clientPort1 := freePort(t)
	clusterPort1 := freePort(t)
	clientPort2 := freePort(t)
	clusterPort2 := freePort(t)

	tl := testLogger{t}
	s1 := New(Options{
		ServerName:   "pve01",
		ClientHost:   "127.0.0.1",
		ClientPort:   clientPort1,
		ClusterHost:  "127.0.0.1",
		ClusterPort:  clusterPort1,
		TLS:          l1.Server(),
		ClusterTLS:   l1.Cluster(),
		StaticRoutes: []string{fmt.Sprintf("nats-route://127.0.0.1:%d", clusterPort2)},
		Logger:       tl,
	})
	s2 := New(Options{
		ServerName:   "pve02",
		ClientHost:   "127.0.0.1",
		ClientPort:   clientPort2,
		ClusterHost:  "127.0.0.1",
		ClusterPort:  clusterPort2,
		TLS:          l2.Server(),
		ClusterTLS:   l2.Cluster(),
		StaticRoutes: []string{fmt.Sprintf("nats-route://127.0.0.1:%d", clusterPort1)},
		Logger:       tl,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s1.Start(ctx); err != nil {
		t.Fatalf("s1 start: %v", err)
	}
	defer s1.Stop()
	if err := s2.Start(ctx); err != nil {
		t.Fatalf("s2 start: %v", err)
	}
	defer s2.Stop()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if countRoutes(t, s1) >= 1 && countRoutes(t, s2) >= 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if countRoutes(t, s1) == 0 || countRoutes(t, s2) == 0 {
		t.Fatalf("cluster routes did not establish: s1=%d s2=%d", countRoutes(t, s1), countRoutes(t, s2))
	}
}

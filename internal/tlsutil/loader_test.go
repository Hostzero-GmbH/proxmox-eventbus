package tlsutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTestCA(t *testing.T, dir string) (caPath, keyPath string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
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
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	caPath = filepath.Join(dir, "ca.pem")
	keyPath = filepath.Join(dir, "ca.key")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		t.Fatal(err)
	}
	kb, _ := x509.MarshalPKCS8PrivateKey(priv)
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kb}), 0o600); err != nil {
		t.Fatal(err)
	}
	return
}

func TestIssueClientCert(t *testing.T) {
	dir := t.TempDir()
	caPath, keyPath := writeTestCA(t, dir)
	out := filepath.Join(dir, "out")
	err := IssueClientCert(IssueClientCertOpts{
		CACertFile: caPath,
		CAKeyFile:  keyPath,
		CN:         "floating-ip-agent",
		OutDir:     out,
		Validity:   time.Hour,
	})
	if err != nil {
		t.Fatalf("IssueClientCert: %v", err)
	}
	for _, name := range []string{"client.pem", "client.key", "ca.pem"} {
		if _, err := os.Stat(filepath.Join(out, name)); err != nil {
			t.Errorf("missing %s: %v", name, err)
		}
	}
}

func TestLoaderReload(t *testing.T) {
	dir := t.TempDir()
	caPath, _ := writeTestCA(t, dir)
	priv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "node1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	caCert, _ := loadPEMCert(caPath)
	caKey, _ := loadPEMKey(filepath.Join(dir, "ca.key"))
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &priv.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	certPath := filepath.Join(dir, "node1.pem")
	keyPath := filepath.Join(dir, "node1.key")
	_ = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644)
	kb, _ := x509.MarshalPKCS8PrivateKey(priv)
	_ = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kb}), 0o600)

	l, err := NewLoader(caPath, certPath, keyPath, true)
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}
	got := l.Server()
	if got.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("ClientAuth = %v, want RequireAndVerifyClientCert", got.ClientAuth)
	}
	if err := l.Reload(); err != nil {
		t.Errorf("Reload: %v", err)
	}
}

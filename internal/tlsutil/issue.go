package tlsutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

const (
	DefaultCAcertPath = "/etc/pve/pve-root-ca.pem"
	DefaultCAkeyPath  = "/etc/pve/priv/pve-root-ca.key"
)

// IssueClientCertOpts controls IssueClientCert.
type IssueClientCertOpts struct {
	CACertFile string
	CAKeyFile  string
	CN         string
	OutDir     string
	Validity   time.Duration
	Orgs       []string
}

// ClientCertBundle is the in-memory result of IssueClientCertPEM: PEM-encoded
// CA, client cert, and PKCS#8 EC private key, ready to feed into tls.Config.
type ClientCertBundle struct {
	CAPEM   []byte
	CertPEM []byte
	KeyPEM  []byte
}

// IssueClientCertPEM mints a short-lived client certificate signed by the PVE
// cluster CA and returns the PEM blobs without touching the filesystem.
// Must be run as root since /etc/pve/priv/pve-root-ca.key is root-only.
func IssueClientCertPEM(opts IssueClientCertOpts) (*ClientCertBundle, error) {
	if opts.CACertFile == "" {
		opts.CACertFile = DefaultCAcertPath
	}
	if opts.CAKeyFile == "" {
		opts.CAKeyFile = DefaultCAkeyPath
	}
	if opts.CN == "" {
		return nil, errors.New("CN required")
	}
	if opts.Validity == 0 {
		opts.Validity = 365 * 24 * time.Hour
	}
	if len(opts.Orgs) == 0 {
		opts.Orgs = []string{"proxmox-eventbus client"}
	}

	caCert, err := loadPEMCert(opts.CACertFile)
	if err != nil {
		return nil, fmt.Errorf("load CA cert: %w", err)
	}
	caKey, err := loadPEMKey(opts.CAKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load CA key: %w", err)
	}

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   opts.CN,
			Organization: opts.Orgs,
		},
		NotBefore:   time.Now().Add(-1 * time.Minute),
		NotAfter:    time.Now().Add(opts.Validity),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &priv.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("sign cert: %w", err)
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, err
	}
	caPEM, err := os.ReadFile(opts.CACertFile)
	if err != nil {
		return nil, fmt.Errorf("read CA cert: %w", err)
	}

	return &ClientCertBundle{
		CAPEM:   caPEM,
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	}, nil
}

// IssueClientCert mints a short-lived client certificate signed by the PVE
// cluster CA and writes it to opts.OutDir as ca.pem, client.pem, client.key.
func IssueClientCert(opts IssueClientCertOpts) error {
	if opts.OutDir == "" {
		return errors.New("OutDir required")
	}
	bundle, err := IssueClientCertPEM(opts)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(opts.OutDir, "ca.pem"), bundle.CAPEM, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(opts.OutDir, "client.pem"), bundle.CertPEM, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(opts.OutDir, "client.key"), bundle.KeyPEM, 0o600); err != nil {
		return err
	}
	return nil
}

func loadPEMCert(path string) (*x509.Certificate, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	for {
		var block *pem.Block
		block, b = pem.Decode(b)
		if block == nil {
			return nil, errors.New("no CERTIFICATE block")
		}
		if block.Type == "CERTIFICATE" {
			return x509.ParseCertificate(block.Bytes)
		}
	}
}

func loadPEMKey(path string) (any, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	for {
		var block *pem.Block
		block, b = pem.Decode(b)
		if block == nil {
			return nil, errors.New("no key block")
		}
		switch block.Type {
		case "RSA PRIVATE KEY":
			return x509.ParsePKCS1PrivateKey(block.Bytes)
		case "EC PRIVATE KEY":
			return x509.ParseECPrivateKey(block.Bytes)
		case "PRIVATE KEY":
			return x509.ParsePKCS8PrivateKey(block.Bytes)
		}
	}
}

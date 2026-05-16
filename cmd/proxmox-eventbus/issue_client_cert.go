package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/tlsutil"
)

func runIssueClientCert(args []string) int {
	fs := flag.NewFlagSet("issue-client-cert", flag.ContinueOnError)
	cn := fs.String("cn", "", "common name (required); the consumer identity")
	out := fs.String("out", "", "output directory (required); will be created")
	caCert := fs.String("ca-cert", tlsutil.DefaultCAcertPath, "PVE cluster CA cert")
	caKey := fs.String("ca-key", tlsutil.DefaultCAkeyPath, "PVE cluster CA private key (root-readable)")
	validity := fs.Duration("validity", 365*24*time.Hour, "certificate validity (e.g. 720h)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *cn == "" || *out == "" {
		fs.Usage()
		fmt.Fprintln(os.Stderr, "--cn and --out are required")
		return 2
	}
	if err := tlsutil.IssueClientCert(tlsutil.IssueClientCertOpts{
		CACertFile: *caCert,
		CAKeyFile:  *caKey,
		CN:         *cn,
		OutDir:     *out,
		Validity:   *validity,
	}); err != nil {
		fmt.Fprintln(os.Stderr, "issue-client-cert:", err)
		return 1
	}
	fmt.Printf("Wrote client certificate bundle to %s\n", *out)
	fmt.Println("  ca.pem      cluster CA")
	fmt.Println("  client.pem  client certificate")
	fmt.Println("  client.key  client private key (mode 0600)")
	return 0
}

package bus

import (
	"fmt"
	"strings"
)

// natsLogAdapter implements natsserver/server.Logger by forwarding to our Logger.
//
// It also suppresses a nats-server warning that fires whenever cluster TLS is
// configured with InsecureSkipVerify=true. We intentionally enable that to
// disable Go's default hostname check for cluster routes (PVE per-node certs
// don't carry cluster-network IPs in their SAN); chain verification is
// instead performed in VerifyPeerCertificate against the PVE cluster CA.
// The upstream message would mislead operators into thinking we don't verify.
type natsLogAdapter struct{ l Logger }

const suppressedClusterTLSWarning = "TLS certificate chain and hostname of solicited routes will not be verified."

func (a *natsLogAdapter) Noticef(format string, v ...any) { a.l.Info(fmt.Sprintf(format, v...)) }
func (a *natsLogAdapter) Warnf(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	if strings.Contains(msg, suppressedClusterTLSWarning) {
		return
	}
	a.l.Warn(msg)
}
func (a *natsLogAdapter) Fatalf(format string, v ...any) { a.l.Error(fmt.Sprintf(format, v...)) }
func (a *natsLogAdapter) Errorf(format string, v ...any) { a.l.Error(fmt.Sprintf(format, v...)) }
func (a *natsLogAdapter) Debugf(format string, v ...any) { a.l.Debug(fmt.Sprintf(format, v...)) }
func (a *natsLogAdapter) Tracef(format string, v ...any) { a.l.Debug(fmt.Sprintf(format, v...)) }

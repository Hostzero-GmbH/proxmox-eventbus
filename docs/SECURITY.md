# Security model

## TLS PKI

`proxmox-eventbus` reuses the cluster PKI that PVE provisions itself:

- CA cert: `/etc/pve/pve-root-ca.pem` (replicated by pmxcfs to every node,
  readable by `www-data`).
- CA key: `/etc/pve/priv/pve-root-ca.key` (replicated by pmxcfs to every
  node, **root-only**).
- Node cert/key: `/etc/pve/local/pve-ssl.{pem,key}` (per-node, key readable
  by `www-data`).

Both NATS listeners (`:4222` for clients and `:6222` for cluster routes)
require mTLS and verify the peer against the cluster CA. There is no separate
bootstrap step; `pvecm add` on a new node provisions a usable cert and the
daemon picks it up.

### Hostname/IP check is disabled on cluster routes (by design)

PVE issues `pve-ssl.pem` with a SAN containing the node short name and the
management-interface IP. In multi-network clusters the daemon is reached over
a *different* address (e.g. a dedicated cluster/migration network from
`/etc/pve/.members`), which is **not** in the SAN. Go's default TLS
verification would reject this even though the cert is perfectly valid.

The cluster listener (`:6222`) therefore:

1. Sets `InsecureSkipVerify = true` to switch off Go's hostname check.
2. Sets `ClientAuth = RequireAnyClientCert` to switch off Go's default chain
   check on the server side.
3. Performs strict chain verification ourselves in `VerifyPeerCertificate`
   against `/etc/pve/pve-root-ca.pem`.

Trust boundary: **any cert signed by the cluster CA is a valid peer**, regardless
of which IP it presents. This matches PVE's existing PKI trust model (the CA
key is root-only inside pmxcfs, so possession of a signed cert already implies
root on a cluster node).

`nats-server` logs a generic warning when `InsecureSkipVerify=true`; the daemon
filters it because it's misleading in our setup (chain verification is still
performed). Our log adapter prints an accurate line at startup instead:

```
cluster route TLS: chain verified against CA, hostname check disabled
```

The client listener (`:4222`) keeps full default Go verification on inbound
client certs, because external consumers don't have the cluster-IP-in-SAN
problem and the standard mTLS check is appropriate there.

Key reach:

- The daemon runs as `proxmox-eventbus:www-data`. It can read the local
  `pve-ssl.key` (PVE uses the same group for `pveproxy`).
- The CA private key in `/etc/pve/priv/` is **never** read by the daemon at
  runtime - only by the explicit `issue-client-cert` subcommand, which must be
  invoked as root.

## External consumer certificates

External consumers (e.g. `floating-ip-agent`) need a client cert signed by the
cluster CA. Mint one on any node (run as root):

```
sudo proxmox-eventbus issue-client-cert --cn floating-ip-agent --out ./bundle
```

That writes three files into `./bundle/`:

- `ca.pem` - the cluster CA (consumer's `RootCAs`)
- `client.pem` - the new client cert
- `client.key` - the new client private key (mode 0600)

Default validity is 365 days; pass `--validity 720h` etc. to override.

## Cert rotation

`pvecm updatecerts` rewrites `/etc/pve/local/pve-ssl.*`. The daemon detects
this and hot-reloads the in-memory `tls.Config`. Existing sessions remain
valid; new connections use the new cert.

## Threat model

In-scope:

- Tampering on the wire between cluster nodes (defeated by mTLS).
- Unauthorized consumers connecting to NATS (defeated by `verify_client`).
- Privilege escalation via the daemon process (mitigated by systemd
  sandboxing - see [packaging/systemd/proxmox-eventbus.service](../packaging/systemd/proxmox-eventbus.service)).

## Linux capabilities

The daemon drops all capabilities except `CAP_DAC_READ_SEARCH`, which it needs
to read `/run/qemu-server/<vmid>.pid` (PVE writes those mode 0600 root:root).
The cap is read-only - the daemon cannot write to root-owned files, connect
to root-owned sockets, or override execute checks. Blast radius if the daemon
is compromised: information disclosure of root-readable files. There is no
write side and no signal/ptrace capability.

QMP introspection (`snapshot.qmp: true`) is intentionally **not** supported in
this configuration: connecting to `/run/qemu-server/<vmid>.qmp` (mode 0750
root:root) requires write access on the socket and therefore `CAP_DAC_OVERRIDE`.
Enable QMP only if you accept that broader capability and add it to the unit
manually.

Out of scope:

- A compromised PVE node. With root access on any node the attacker already
  has access to the CA key in pmxcfs and could mint arbitrary client certs.
- Auditing / per-cert revocation. The cluster CA is shared with PVE itself;
  rotating it requires `pvecm updatecerts` and impacts the whole cluster.
  Future versions may add a separate, daemon-managed CA for downstream
  consumers.

## Subject ACLs

NATS subjects are unrestricted in v1: any client cert signed by the cluster
CA can subscribe to any subject and publish to `pve.<cluster>.snapshot.request`
or `pve.<cluster>.<node>.snapshot.request` to trigger snapshots.

If you want per-consumer ACLs, run a separate NATS server in front of the
embedded cluster (configurable via the daemon's `static_routes`) or wait
for the upcoming `acls:` config block.

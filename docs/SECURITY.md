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

# proxmox-eventbus

A low-latency event bus daemon for Proxmox VE clusters. It runs on every PVE
node, watches the local task log via inotify, enriches each event from
`/etc/pve`, and publishes CloudEvents on an embedded NATS server that
auto-clusters with all peer nodes using the cluster mTLS PKI that PVE already
provisions.

The whole point is to make VM and container lifecycle changes visible to other
software within tens of milliseconds, without polling the Proxmox HTTP API or
tailing `pvesh` logs.

- Lifecycle latency from `qm start` to event published: typically 10-50 ms.
- Embedded NATS with full mesh between PVE nodes - no broker to deploy or
  operate, no LB, no DNS SRV.
- mTLS reuses `/etc/pve/pve-root-ca.pem` and `/etc/pve/local/pve-ssl.{pem,key}`,
  so a fresh `pvecm add` makes the new node a working bus member immediately.
- Periodic snapshots (general interrogation) every 30 s plus on-demand
  interrogation, so consumers can rebuild state and detect missed events.
- CloudEvents 1.0 JSON with a stable subject layout - any NATS client works.

Use cases:

- Floating-IP failover (see [the example below](#use-case-floating-ip-failover))
- HA orchestration sidecars
- Audit / SIEM ingestion
- Inventory and topology agents that mirror VM placement

## Install

Each release attaches signed `.deb`s for `amd64` and `arm64`.

```
curl -fsSLO https://github.com/Hostzero-GmbH/proxmox-eventbus/releases/download/v0.1.0/proxmox-eventbus_0.1.0_amd64.deb
sudo dpkg -i proxmox-eventbus_0.1.0_amd64.deb
```

That's it. The post-install hook creates the `proxmox-eventbus` user, adds it
to `www-data` (so it can read `/etc/pve`), and starts the systemd unit. No
configuration is required for default clusters - `/etc/proxmox-eventbus/config.yaml`
ships a sensible default.

Verify:

```
systemctl status proxmox-eventbus
journalctl -u proxmox-eventbus -f
```

Live-tail events without setting up consumer credentials:

```
ssh root@pve01 proxmox-eventbus tail 'pve.>'
```

This subscribes to the local NATS server using an ephemeral client cert minted
in-memory from the PVE cluster CA. Add `--json` to get raw CloudEvents one per
line (pipe through `jq`).

## Consuming events

External consumers connect to any combination of node URLs. The NATS client
discovers the rest of the cluster via `INFO` gossip and auto-reconnects.

Mint a client certificate (run as root on any cluster node - this signs with
the PVE cluster CA):

```
sudo proxmox-eventbus issue-client-cert --cn floating-ip-agent --out ./certs
```

That produces `ca.pem`, `client.pem` and `client.key`. Connect from Go:

```go
nc, err := nats.Connect(
    "tls://pve1:4222,tls://pve2:4222,tls://pve3:4222",
    nats.ClientCert("./certs/client.pem", "./certs/client.key"),
    nats.RootCAs("./certs/ca.pem"),
    nats.Name("floating-ip-agent"),
)
```

Subjects:

```
pve.<cluster>.<node>.<kind>.<vmid>.<action>.<phase>
pve.<cluster>.<node>.snapshot.complete
pve.<cluster>.snapshot.request          (you publish; daemons respond)
pve.<cluster>.<node>.snapshot.request   (you publish; that node responds)
```

`<kind>` is `qemu` or `lxc`, `<phase>` is `started`, `finished`, `failed`, or
`snapshot`. See [docs/EVENTS.md](docs/EVENTS.md) for the full catalogue and
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the latency budget.

## Use case: floating IP failover

A consumer `floating-ip-agent` subscribes to migration-start events:

```go
nc.Subscribe("pve.*.*.qemu.*.migrate.started", func(m *nats.Msg) {
    var ev events.CloudEvent
    _ = json.Unmarshal(m.Data, &ev)
    if !hasTag(ev.Data.Tags, "floating-ip") {
        return
    }
    preallocateOn(ev.Data.TargetNode, ev.Data.VMID)
})
nc.Subscribe("pve.*.*.qemu.*.migrate.finished", func(m *nats.Msg) {
    var ev events.CloudEvent
    _ = json.Unmarshal(m.Data, &ev)
    commitOn(ev.Data.TargetNode, ev.Data.VMID)
})
nc.Subscribe("pve.*.*.qemu.*.migrate.failed", func(m *nats.Msg) {
    var ev events.CloudEvent
    _ = json.Unmarshal(m.Data, &ev)
    rollback(ev.Data.VMID)
})
```

Flow when a VM tagged `floating-ip` migrates from `pve1` to `pve2`:

1. Operator (or HA agent) runs `qm migrate 101 pve2 --online`.
2. PVE writes the UPID to `/var/log/pve/tasks/active`; inotify fires within
   tens of microseconds.
3. `proxmox-eventbus` on `pve1` parses the UPID, reads
   `/proc/<pid>/cmdline` to extract the target (`pve2`), enriches the event
   with `name` and `tags` from `/etc/pve/qemu-server/101.conf`, and publishes
   `pve.<cluster>.pve1.qemu.101.migrate.started`. Total: ~10-30 ms.
4. `floating-ip-agent` on `pve2` (connected via NATS cluster routing) receives
   the event, pre-allocates the floating IP, and stages the BGP/ARP update.
5. When migration completes, `migrate.finished` fires; the agent flips traffic
   over instantly. On `migrate.failed` it rolls back.

For cold-start, the agent publishes an empty message on
`pve.<cluster>.snapshot.request` and every node immediately responds with a
full snapshot of its VMs (running/stopped/paused), so the agent knows the
current placement of every tagged VM without polling.

## Configuration

See [packaging/etc/proxmox-eventbus/config.yaml](packaging/etc/proxmox-eventbus/config.yaml)
for every knob. Sensible defaults mean most clusters don't need to touch it.

## Building from source

```
go build ./cmd/proxmox-eventbus
go test ./...
```

Producing a `.deb` locally:

```
ARCH=amd64 VERSION=dev nfpm package --config packaging/nfpm.yaml --packager deb --target dist/
```

## License

Apache-2.0. See [LICENSE](LICENSE).

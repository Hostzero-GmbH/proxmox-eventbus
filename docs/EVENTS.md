# Event catalogue

All events conform to [CloudEvents 1.0](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md)
and are encoded as JSON. The on-wire envelope is the standard CloudEvents
shape; everything proxmox-eventbus specific lives in the `data` field.

## Envelope

| Field | Example | Notes |
|------|---------|-------|
| `specversion` | `"1.0"` | Pinned |
| `id` | `"0c0b9a4e-..."` | UUIDv4, unique per event |
| `source` | `"/pve/hzero/pve1"` | Cluster + originating node |
| `type` | `"dev.proxmox.eventbus.qemu.migrate.started"` | See below |
| `subject` | `"qemu/101"` or `"node/pve1"` | Stable handle for the entity |
| `time` | `"2026-05-16T13:09:00.123Z"` | UTC, RFC 3339 |
| `datacontenttype` | `"application/json"` | |
| `data` | (object) | Schema-versioned, see below |

## `data` schema

```jsonc
{
  "schemaVersion": "1",
  "cluster": "hzero",
  "node": "pve1",

  // lifecycle and snapshot
  "kind": "qemu" | "lxc",
  "vmid": 101,
  "name": "web-01",
  "tags": ["prod", "floating-ip"],
  "description": "...",
  "hookscript": "local:snippets/migrate.sh",

  "action": "start|stop|shutdown|reboot|reset|suspend|resume|migrate|clone|create|destroy|template|state",
  "phase":  "started|synced|finished|failed|snapshot",

  // lifecycle-only
  "upid": "UPID:pve1:00001234:0ABCDEF0:6645A1B2:qmigrate:101:root@pam:",
  "user": "root@pam",
  "source_node": "pve1",
  "target_node": "pve2",
  "duration_ms": 4321,
  "exit_status": "OK",

  // snapshot-only
  "state": "running|stopped|paused|suspended|unknown",
  "snapshot_id": "01HXYZ...",
  "observed_at_ns": 1737028800123456789,

  // snapshot.complete-only
  "count": 42
}
```

## Lifecycle events

Subject:

```
pve.<cluster>.<node>.<kind>.<vmid>.<action>.<phase>
```

Type:

```
dev.proxmox.eventbus.<kind>.<action>.<phase>
```

Each lifecycle action emits two events: `phase=started` when the UPID first
appears in `/var/log/pve/tasks/active`, and `phase=finished` / `phase=failed`
when the matching entry shows a result. QEMU migrations emit one extra,
non-terminal `phase=synced` event in between - see "Migration extras" below.

### Action -> PVE worker types

| Action | QEMU worker | LXC worker |
|---|---|---|
| `start` | `qmstart` | `vzstart` |
| `stop` | `qmstop` | `vzstop` |
| `shutdown` | `qmshutdown` | `vzshutdown` |
| `reboot` | `qmreboot` | `vzreboot` |
| `reset` | `qmreset` | - |
| `suspend` | `qmsuspend` | `vzsuspend` |
| `resume` | `qmresume` | `vzresume` |
| `migrate` | `qmigrate` | `vzmigrate` |
| `clone` | `qmclone` | `vzclone` |
| `create` | `qmcreate` | `vzcreate` |
| `destroy` | `qmdestroy` | `vzdestroy` |
| `template` | `qmtemplate` | - |

### Migration extras

For `action=migrate, phase=started`, `target_node` is populated from
`/proc/<pid>/cmdline` of the spawning `qm migrate` / `pct migrate` process.
For HA-driven migrations where the PID is `pve-ha-lrm`, the field may be
empty on `started`; the `finished` event always has a `duration_ms`.

QEMU live migrations additionally emit `action=migrate, phase=synced` - a
non-terminal event fired the moment the per-task log shows `migration
status: completed`. That line marks memory-state handover to the target;
PVE still spends a few more seconds tearing down the NBD mirror, flushing
conntrack state and removing the source volume before the task's terminal
status (and the `finished` event) appears. Consumers that want to react to
the VM actually running on the target as early as possible should use
`phase=synced` instead of waiting for `phase=finished`. It always precedes
`finished`/`failed` for the same UPID and is only emitted once. LXC
migrations (restart-based, no live memory transfer) never emit it.

## Snapshot events (general interrogation)

These are emitted periodically (default every 30 s with +/- 10% jitter) and on
demand. They let consumers detect lost lifecycle events and rebuild state on
cold start.

| Event | Subject | Type |
|---|---|---|
| Per-VM/CT state | `pve.<cluster>.<node>.<kind>.<vmid>.state.snapshot` | `dev.proxmox.eventbus.<kind>.state.snapshot` |
| Batch completion | `pve.<cluster>.<node>.snapshot.complete` | `dev.proxmox.eventbus.snapshot.complete` |

All events of one tick share a `snapshot_id` (ULID). The `snapshot.complete`
event carries `count` so consumers can confirm the entire batch arrived.

### On-demand interrogation

Publish an empty message on either subject below to trigger an immediate
snapshot:

```
pve.<cluster>.snapshot.request           # every node responds
pve.<cluster>.<node>.snapshot.request    # one node responds
```

## Reconciliation rules (consumer-side)

NATS Core does not guarantee ordering across publishers. Consumers must
reconcile by timestamp:

- Lifecycle events: use `time`.
- Snapshot events: use `observed_at_ns` (nanosecond monotonic per node).
- Per `(kind, vmid)`, the freshest observation wins.

## Versioning

`data.schemaVersion` starts at `"1"`. Additive fields will not bump it.
Breaking changes will bump it and ship a new major release of the daemon.

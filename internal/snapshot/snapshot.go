package snapshot

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/events"
	"github.com/Hostzero-GmbH/proxmox-eventbus/internal/pmxcfs"
)

type Emitter struct {
	Cluster  string
	Node     string
	Interval time.Duration
	JitterPC int

	Reader *pmxcfs.Reader
	Probe  Prober

	Out chan<- events.CloudEvent
}

// Run emits one batch per tick until ctx is cancelled. If Interval is 0, it
// emits a single batch immediately and returns.
func (e *Emitter) Run(ctx context.Context) error {
	if e.Interval <= 0 {
		e.emitBatch()
		return nil
	}
	jitter := time.Duration(0)
	if e.JitterPC > 0 {
		jitterMax := int64(e.Interval) * int64(e.JitterPC) / 100
		if jitterMax > 0 {
			jitter = time.Duration(rand.Int64N(jitterMax))
		}
	}
	first := time.NewTimer(jitter)
	defer first.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-first.C:
	}
	t := time.NewTicker(e.Interval)
	defer t.Stop()
	e.emitBatch()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			e.emitBatch()
		}
	}
}

// EmitOnce emits a single batch immediately. Used by the on-demand interrogation handler.
func (e *Emitter) EmitOnce() { e.emitBatch() }

func (e *Emitter) emitBatch() {
	id := uuid.NewString()
	qemu, lxc, err := e.Reader.VMList()
	if err != nil {
		return
	}
	count := 0

	for _, vmid := range qemu {
		state, detail := e.Probe.QEMUState(vmid)
		ev := e.snapshotEvent(events.KindQEMU, vmid, state, detail, id)
		e.send(ev)
		count++
	}
	for _, vmid := range lxc {
		state, detail := e.Probe.LXCState(vmid)
		ev := e.snapshotEvent(events.KindLXC, vmid, state, detail, id)
		e.send(ev)
		count++
	}

	complete := events.New(
		events.TypeSnapshotComplete(),
		events.SourceFor(e.Cluster, e.Node),
		"node/"+e.Node,
		events.EventData{
			Cluster:       e.Cluster,
			Node:          e.Node,
			SnapshotID:    id,
			SnapshotCount: count,
			ObservedAtNS:  time.Now().UnixNano(),
		},
	)
	e.send(complete)
}

func (e *Emitter) snapshotEvent(kind events.Kind, vmid int, state events.State, detail, snapshotID string) events.CloudEvent {
	data := events.EventData{
		Cluster:      e.Cluster,
		Node:         e.Node,
		Kind:         kind,
		VMID:         vmid,
		Action:       events.ActionState,
		Phase:        events.PhaseSnapshot,
		State:        state,
		StateDetail:  detail,
		SnapshotID:   snapshotID,
		ObservedAtNS: time.Now().UnixNano(),
	}
	if cfg, err := e.Reader.ReadVMConfig(pmxcfs.Kind(kind), vmid); err == nil {
		data.Name = cfg.Name
		data.Tags = cfg.Tags
	}
	return events.New(
		events.TypeFor(kind, events.ActionState, events.PhaseSnapshot),
		events.SourceFor(e.Cluster, e.Node),
		events.SubjectFor(kind, vmid),
		data,
	)
}

func (e *Emitter) send(ev events.CloudEvent) {
	select {
	case e.Out <- ev:
	default:
		// drop on backpressure rather than block the snapshot loop
	}
}

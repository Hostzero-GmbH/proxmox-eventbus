// Package events defines the CloudEvents 1.0 schema published on NATS.
//
// Subjects follow pve.<cluster>.<node>.<kind>.<vmid>.<action>.<phase> for
// lifecycle events, and pve.<cluster>.<node>.snapshot.complete for snapshot
// batch markers. The CloudEvents `type` mirrors that structure but uses the
// reverse-DNS form dev.proxmox.eventbus.<kind>.<action>.<phase>.
package events

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	SpecVersion        = "1.0"
	TypeNamespace      = "dev.proxmox.eventbus"
	DataContentType    = "application/json"
	SchemaVersion      = "1"
	SourcePathTemplate = "/pve/%s/%s" // /pve/<cluster>/<node>
)

type Kind string

const (
	KindQEMU Kind = "qemu"
	KindLXC  Kind = "lxc"
)

type Phase string

const (
	PhaseStarted  Phase = "started"
	PhaseFinished Phase = "finished"
	PhaseFailed   Phase = "failed"
	PhaseSnapshot Phase = "snapshot"
)

type Action string

const (
	ActionStart    Action = "start"
	ActionStop     Action = "stop"
	ActionShutdown Action = "shutdown"
	ActionReboot   Action = "reboot"
	ActionReset    Action = "reset"
	ActionSuspend  Action = "suspend"
	ActionResume   Action = "resume"
	ActionMigrate  Action = "migrate"
	ActionClone    Action = "clone"
	ActionCreate   Action = "create"
	ActionDestroy  Action = "destroy"
	ActionTemplate Action = "template"
	// ActionState is the pseudo-action carried by snapshot events.
	ActionState Action = "state"
)

type State string

const (
	StateRunning   State = "running"
	StateStopped   State = "stopped"
	StatePaused    State = "paused"
	StateSuspended State = "suspended"
	StateUnknown   State = "unknown"
)

// CloudEvent is the on-wire envelope.
type CloudEvent struct {
	SpecVersion     string    `json:"specversion"`
	ID              string    `json:"id"`
	Source          string    `json:"source"`
	Type            string    `json:"type"`
	Subject         string    `json:"subject,omitempty"`
	Time            time.Time `json:"time"`
	DataContentType string    `json:"datacontenttype"`
	Data            EventData `json:"data"`
}

// EventData is the strongly-typed CloudEvents `data` payload.
// One struct covers lifecycle, snapshot and snapshot-complete events; unused
// fields are omitted via `omitempty`.
type EventData struct {
	SchemaVersion string   `json:"schemaVersion"`
	Cluster       string   `json:"cluster"`
	Node          string   `json:"node"`

	// Lifecycle + snapshot fields
	Kind        Kind     `json:"kind,omitempty"`
	VMID        int      `json:"vmid,omitempty"`
	Name        string   `json:"name,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	Description string   `json:"description,omitempty"`
	HookScript  string   `json:"hookscript,omitempty"`

	Action Action `json:"action,omitempty"`
	Phase  Phase  `json:"phase,omitempty"`

	// Lifecycle-only
	UPID        string `json:"upid,omitempty"`
	User        string `json:"user,omitempty"`
	SourceNode  string `json:"source_node,omitempty"`
	TargetNode  string `json:"target_node,omitempty"`
	DurationMS  *int64 `json:"duration_ms,omitempty"`
	ExitStatus  string `json:"exit_status,omitempty"`

	// Snapshot-only
	State         State  `json:"state,omitempty"`
	StateDetail   string `json:"state_detail,omitempty"` // populated when state=unknown to surface the cause
	SnapshotID    string `json:"snapshot_id,omitempty"`
	ObservedAtNS  int64  `json:"observed_at_ns,omitempty"`

	// snapshot.complete-only
	SnapshotCount int `json:"count,omitempty"`
}

// SubjectLifecycle builds pve.<cluster>.<node>.<kind>.<vmid>.<action>.<phase>.
func SubjectLifecycle(cluster, node string, kind Kind, vmid int, action Action, phase Phase) string {
	return fmt.Sprintf("pve.%s.%s.%s.%d.%s.%s",
		safe(cluster), safe(node), kind, vmid, action, phase)
}

// SubjectSnapshotComplete builds pve.<cluster>.<node>.snapshot.complete.
func SubjectSnapshotComplete(cluster, node string) string {
	return fmt.Sprintf("pve.%s.%s.snapshot.complete", safe(cluster), safe(node))
}

// SubjectSnapshotRequestCluster builds pve.<cluster>.snapshot.request.
func SubjectSnapshotRequestCluster(cluster string) string {
	return fmt.Sprintf("pve.%s.snapshot.request", safe(cluster))
}

// SubjectSnapshotRequestNode builds pve.<cluster>.<node>.snapshot.request.
func SubjectSnapshotRequestNode(cluster, node string) string {
	return fmt.Sprintf("pve.%s.%s.snapshot.request", safe(cluster), safe(node))
}

// TypeFor returns the CloudEvents `type` attribute.
func TypeFor(kind Kind, action Action, phase Phase) string {
	return fmt.Sprintf("%s.%s.%s.%s", TypeNamespace, kind, action, phase)
}

func TypeSnapshotComplete() string {
	return TypeNamespace + ".snapshot.complete"
}

// SourceFor returns the CloudEvents `source` URI.
func SourceFor(cluster, node string) string {
	return fmt.Sprintf(SourcePathTemplate, safe(cluster), safe(node))
}

// SubjectFor returns the CloudEvents `subject` attribute for VM-scoped events.
func SubjectFor(kind Kind, vmid int) string {
	return fmt.Sprintf("%s/%d", kind, vmid)
}

// New constructs a CloudEvent with id and time populated.
func New(typ, source, subject string, data EventData) CloudEvent {
	data.SchemaVersion = SchemaVersion
	return CloudEvent{
		SpecVersion:     SpecVersion,
		ID:              uuid.NewString(),
		Source:          source,
		Type:            typ,
		Subject:         subject,
		Time:            time.Now().UTC(),
		DataContentType: DataContentType,
		Data:            data,
	}
}

// NATS subjects are limited to printable ASCII without dots-as-tokens or whitespace.
// Sanitize anything we don't expect in a cluster or node name.
func safe(s string) string {
	if s == "" {
		return "default"
	}
	b := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z',
			c >= 'A' && c <= 'Z',
			c >= '0' && c <= '9',
			c == '-', c == '_':
			b = append(b, c)
		default:
			b = append(b, '_')
		}
	}
	return string(b)
}

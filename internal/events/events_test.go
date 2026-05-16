package events

import (
	"encoding/json"
	"testing"
)

func TestSubjectAndType(t *testing.T) {
	s := SubjectLifecycle("hzero", "pve1", KindQEMU, 101, ActionMigrate, PhaseStarted)
	if s != "pve.hzero.pve1.qemu.101.migrate.started" {
		t.Errorf("subject = %q", s)
	}
	if got := TypeFor(KindQEMU, ActionMigrate, PhaseStarted); got != "dev.proxmox.eventbus.qemu.migrate.started" {
		t.Errorf("type = %q", got)
	}
	if got := SourceFor("hzero", "pve1"); got != "/pve/hzero/pve1" {
		t.Errorf("source = %q", got)
	}
}

func TestSubjectSanitisation(t *testing.T) {
	got := SubjectLifecycle("with space.dots", "pve.1", KindQEMU, 1, ActionStart, PhaseStarted)
	if got != "pve.with_space_dots.pve_1.qemu.1.start.started" {
		t.Errorf("unsanitised: %q", got)
	}
}

func TestCloudEventJSON(t *testing.T) {
	ev := New(
		TypeFor(KindQEMU, ActionMigrate, PhaseStarted),
		SourceFor("hzero", "pve1"),
		SubjectFor(KindQEMU, 101),
		EventData{
			Cluster: "hzero", Node: "pve1",
			Kind: KindQEMU, VMID: 101,
			Action: ActionMigrate, Phase: PhaseStarted,
			SourceNode: "pve1", TargetNode: "pve2",
		},
	)
	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	var back CloudEvent
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("roundtrip: %v", err)
	}
	if back.Data.TargetNode != "pve2" {
		t.Errorf("target_node lost in roundtrip: %+v", back.Data)
	}
	if back.SpecVersion != "1.0" {
		t.Errorf("specversion = %q", back.SpecVersion)
	}
}

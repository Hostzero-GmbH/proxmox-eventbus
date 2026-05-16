package tasks

import "github.com/Hostzero-GmbH/proxmox-eventbus/internal/events"

// workerTypes maps PVE worker type strings to (kind, action).
var workerTypes = map[string]struct {
	Kind   events.Kind
	Action events.Action
}{
	"qmstart":    {events.KindQEMU, events.ActionStart},
	"qmstop":     {events.KindQEMU, events.ActionStop},
	"qmshutdown": {events.KindQEMU, events.ActionShutdown},
	"qmreboot":   {events.KindQEMU, events.ActionReboot},
	"qmreset":    {events.KindQEMU, events.ActionReset},
	"qmsuspend":  {events.KindQEMU, events.ActionSuspend},
	"qmresume":   {events.KindQEMU, events.ActionResume},
	"qmigrate":   {events.KindQEMU, events.ActionMigrate},
	"qmclone":    {events.KindQEMU, events.ActionClone},
	"qmcreate":   {events.KindQEMU, events.ActionCreate},
	"qmdestroy":  {events.KindQEMU, events.ActionDestroy},
	"qmtemplate": {events.KindQEMU, events.ActionTemplate},

	"vzstart":    {events.KindLXC, events.ActionStart},
	"vzstop":     {events.KindLXC, events.ActionStop},
	"vzshutdown": {events.KindLXC, events.ActionShutdown},
	"vzreboot":   {events.KindLXC, events.ActionReboot},
	"vzsuspend":  {events.KindLXC, events.ActionSuspend},
	"vzresume":   {events.KindLXC, events.ActionResume},
	"vzmigrate":  {events.KindLXC, events.ActionMigrate},
	"vzclone":    {events.KindLXC, events.ActionClone},
	"vzcreate":   {events.KindLXC, events.ActionCreate},
	"vzdestroy":  {events.KindLXC, events.ActionDestroy},
}

// WorkerTypeMap resolves a PVE worker type into (kind, action).
func WorkerTypeMap(t string) (events.Kind, events.Action, bool) {
	v, ok := workerTypes[t]
	if !ok {
		return "", "", false
	}
	return v.Kind, v.Action, true
}

/*
Package metric implements an abstraction for safely managing metrics in concurrent environments.
*/
package metric

import "fmt"

const (
	DEFAULT_REGISTRY        string = "default"
	PENDING_EVENTS_REGISTRY string = "pending_events"
)

var instance Manager

func init() {
	reset()
}

func reset() {
	instance = &manager{
		registries: map[string]Registry{},
	}
	instance.(*manager).createRegistry(DEFAULT_REGISTRY)
	instance.(*manager).createRegistry(PENDING_EVENTS_REGISTRY)
}

// GetManager gets the manager singleton
func GetManager() Manager {
	return instance
}

func Reset() {
	reset()
}

// Manager is the entry-point for retrieving metric registries
type Manager interface {
	// GetRegistry gets a registry by its key
	GetRegistry(key string) Registry
}

type manager struct {
	registries map[string]Registry
}

func (r *manager) createRegistry(key string) {
	r.registries[key] = NewRegistry()
}

func (r *manager) GetRegistry(key string) Registry {
	return r.registries[key]
}

func GetPendingEventsMeasurement(tablePrefix string, workspace string, destType string) Gauge {
	return GetManager().GetRegistry(PENDING_EVENTS_REGISTRY).MustGetGauge(&pendingEventsMeasurement{tablePrefix, workspace, destType})
}

type pendingEventsMeasurement struct {
	tablePrefix string
	workspace   string
	destType    string
}

func (r *pendingEventsMeasurement) GetKey() interface{} {
	return *r
}
func (r *pendingEventsMeasurement) GetName() string {
	return fmt.Sprintf("%s_pending_events", r.tablePrefix)
}
func (r *pendingEventsMeasurement) GetTags() map[string]string {
	res := map[string]string{
		"workspace": r.workspace,
		"destType":  r.destType,
	}
	return res
}

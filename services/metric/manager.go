/*
Package metric implements an abstraction for safely managing metrics in concurrent environments.
*/
package metric

const (
	PUBLISHED_METRICS string = "published_metrics"
)

var instance Manager = &manager{
	registries: map[string]Registry{
		PUBLISHED_METRICS: NewRegistry(),
	},
}

// GetManager gets the manager's singleton instance
func GetManager() Manager {
	return instance
}

// Manager is the entry-point for retrieving metric registries
type Manager interface {
	// GetRegistry gets a registry by its key
	GetRegistry(key string) Registry
	// Reset cleans all registries
	Reset()
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

func (r *manager) Reset() {
	for key := range r.registries {
		r.registries[key] = NewRegistry()
	}
}

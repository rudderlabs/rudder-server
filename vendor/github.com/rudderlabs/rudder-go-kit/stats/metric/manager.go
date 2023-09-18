/*
Package metric implements an abstraction for safely managing metrics in concurrent environments.
*/
package metric

const (
	PublishedMetrics string = "published_metrics"
)

func NewManager() Manager {
	return &manager{
		registries: map[string]Registry{
			PublishedMetrics: NewRegistry(),
		},
	}
}

var Instance Manager = NewManager()

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

func (r *manager) GetRegistry(key string) Registry {
	return r.registries[key]
}

func (r *manager) Reset() {
	for key := range r.registries {
		r.registries[key] = NewRegistry()
	}
}

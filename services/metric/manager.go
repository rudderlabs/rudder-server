/*
Package metric implements an abstraction for safely managing metrics in concurrent environments.
*/
package metric

import (
	"sync"
)

const (
	PUBLISHED_METRICS string = "published_metrics"
)

func NewManager() Manager {
	return &manager{
		registries: map[string]Registry{
			PUBLISHED_METRICS: NewRegistry(),
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
	mu         sync.RWMutex
	registries map[string]Registry
}

func (r *manager) GetRegistry(key string) Registry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.registries[key]
}

func (r *manager) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key := range r.registries {
		r.registries[key] = NewRegistry()
	}
}

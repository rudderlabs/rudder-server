package features

import "sync"

var DefaultRegistry = &Registry{}

type Registry struct {
	mu    sync.RWMutex
	store map[string][]string // component -> features
}

func (r *Registry) Register(component string, features ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.store == nil {
		r.store = make(map[string][]string)
	}

	r.store[component] = append(r.store[component], features...)
}

func (r *Registry) Each(fn func(component string, features []string)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for component, features := range r.store {
		fn(component, features)
	}
}

func Register(component string, features ...string) {
	DefaultRegistry.Register(component, features...)
}

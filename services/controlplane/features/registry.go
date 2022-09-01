package features

import "sync"

var (
	DefaultRegistry = &Registry{}
)

type item struct {
	Name     string
	Features []string
}

type Registry struct {
	mu    sync.RWMutex
	store []item
}

func (r *Registry) Register(name string, features []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.store = append(r.store, item{Name: name, Features: features})
}

func (r *Registry) Each(fn func(name string, features []string)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, item := range r.store {
		fn(item.Name, item.Features)
	}
}

func Register(name string, features []string) {
	DefaultRegistry.Register(name, features)
}

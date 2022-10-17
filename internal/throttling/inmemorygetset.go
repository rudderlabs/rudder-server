package throttling

import (
	"sync"
	"time"
)

// TODO expiration mechanism?
type inMemoryGetterSetter struct {
	mu sync.Mutex
	m  map[string]interface{}
	ex map[string]time.Time
}

func (i *inMemoryGetterSetter) Get(key string) (interface{}, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	v, ok := i.m[key]
	if !ok {
		return 0, nil
	}
	if time.Now().UnixNano() > i.ex[key].UnixNano() {
		delete(i.m, key)
		delete(i.ex, key)
		return 0, nil
	}
	return v, nil
}

func (i *inMemoryGetterSetter) Set(key string, value interface{}, expiration int64) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.m == nil {
		i.m = make(map[string]interface{})
		i.ex = make(map[string]time.Time)
	}
	i.m[key] = value
	i.ex[key] = time.Now().Add(time.Duration(expiration) * time.Second)
	return nil
}

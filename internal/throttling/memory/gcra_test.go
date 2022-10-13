package memory

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGCRA(t *testing.T) {
	// @TODO finish this test
	g := gcra{
		getterSetter: &inMemoryGetterSetter{
			m:  make(map[string]interface{}),
			ex: make(map[string]time.Time),
		},
	}
	ticker := time.NewTicker(time.Second)
	for {
		allowed, _, _, _, err := g.limit("test", 1, 1, 1, 5)
		require.NoError(t, err)
		t.Log(allowed)
		<-ticker.C
	}
}

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
	i.m[key] = value
	i.ex[key] = time.Now().Add(time.Duration(expiration) * time.Second)
	return nil
}

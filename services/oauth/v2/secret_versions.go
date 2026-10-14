package v2

import (
	"context"
	"sync"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

// SecretVersions mirrors each account's secretVersion from the workspace config. One per backend
// config, shared by every OAuth handler in the process (see WithReauthEviction).
type SecretVersions struct {
	mu       sync.RWMutex
	versions map[string]int // accountID -> secretVersion in the latest config; 0 when absent
}

func (s *SecretVersions) Get(accountID string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.versions[accountID]
}

var (
	secretVersionsMu sync.Mutex
	secretVersionsBy = map[backendconfig.BackendConfig]*SecretVersions{}
)

// SecretVersionsFor returns the mirror for bc, starting its single watcher on first use.
// The subscription is never cancelled and lives as long as the process, like the backend config itself,
// and entries are never removed: a process has one backend config, and a test that builds many mocks pays
// one small map entry each.
func SecretVersionsFor(bc backendconfig.BackendConfig) *SecretVersions {
	// A handler without a backend config does not evict.
	if bc == nil {
		return nil
	}
	secretVersionsMu.Lock()
	defer secretVersionsMu.Unlock()
	if s, ok := secretVersionsBy[bc]; ok {
		return s
	}
	s := &SecretVersions{versions: map[string]int{}}
	secretVersionsBy[bc] = s
	go s.watch(bc.Subscribe(context.Background(), backendconfig.TopicBackendConfig))
	return s
}

func (s *SecretVersions) watch(ch pubsub.DataChannel) {
	for ev := range ch {
		configs, ok := ev.Data.(map[string]backendconfig.ConfigT)
		if !ok {
			// Every config subscriber in the repo asserts this payload type; a different shape would break the router first.
			continue
		}
		next := make(map[string]int)
		for _, cfg := range configs {
			for accountID, account := range cfg.Accounts {
				next[accountID] = account.SecretVersion
			}
		}
		s.mu.Lock()
		s.versions = next
		s.mu.Unlock()
	}
}

package batchrouter

import "sync"

// secretVersions holds the secretVersion of the delivery accounts of the batch router's destinations,
// as of the latest workspace config. The OAuth handlers of async destinations read it, since their
// manager keeps the destination it was built with until the destination's revision changes.
type secretVersions struct {
	mu       sync.RWMutex
	versions map[string]int // accountID -> secretVersion
}

func (s *secretVersions) SecretVersion(accountID string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.versions[accountID]
}

func (s *secretVersions) set(versions map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.versions = versions
}

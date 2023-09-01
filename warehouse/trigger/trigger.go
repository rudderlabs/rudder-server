package trigger

import (
	"sync"
)

type Store struct {
	identifiersMap     map[string]struct{}
	identifiersMapLock sync.RWMutex
}

func NewStore() *Store {
	return &Store{
		identifiersMap: make(map[string]struct{}),
	}
}

func (s *Store) IsTriggered(identifier string) bool {
	s.identifiersMapLock.RLock()
	_, isTriggered := s.identifiersMap[identifier]
	s.identifiersMapLock.RUnlock()

	return isTriggered
}

func (s *Store) Trigger(identifier string) {
	s.identifiersMapLock.Lock()
	defer s.identifiersMapLock.Unlock()

	s.identifiersMap[identifier] = struct{}{}
}

func (s *Store) ClearTrigger(identifier string) {
	s.identifiersMapLock.Lock()
	defer s.identifiersMapLock.Unlock()

	delete(s.identifiersMap, identifier)
}

package trigger

import (
	"sync"
)

type Store struct {
	identifiersMap sync.Map
}

func NewStore() *Store {
	return &Store{}
}

func (s *Store) IsTriggered(identifier string) bool {
	_, isTriggered := s.identifiersMap.Load(identifier)
	return isTriggered
}

func (s *Store) Trigger(identifier string) {
	s.identifiersMap.Store(identifier, struct{}{})
}

func (s *Store) ClearTrigger(identifier string) {
	s.identifiersMap.Delete(identifier)
}

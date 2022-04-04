package state

import (
	"github.com/rudderlabs/rudder-server/app/cluster"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"
	"sync"
)

var _ cluster.ModeProvider = &StaticProvider{}

type StaticProvider struct {
	Mode   servermode.Mode
	ch     chan servermode.Ack
	chOnce sync.Once
}

func (s *StaticProvider) ServerMode() (<-chan servermode.Ack, error) {
	s.chOnce.Do(func() {
		s.ch = make(chan servermode.Ack, 1)
	})
	s.ch <- servermode.WithACK(servermode.Mode(s.Mode), func() {})
	return s.ch, nil
}

func (s *StaticProvider) Close() {
	if s.ch != nil {
		close(s.ch)
	}
}

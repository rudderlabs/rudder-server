package state

import "github.com/rudderlabs/rudder-server/utils/types/servermode"

type StaticProvider struct {
	Mode servermode.Mode
}

func (s *StaticProvider) ServerMode() <-chan servermode.Ack {
	ch := make(chan servermode.Ack, 1)
	ch <- servermode.WithACK(servermode.Mode(s.Mode), func() {})
	close(ch)
	return ch
}

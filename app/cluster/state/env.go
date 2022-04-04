package state

import (
	"fmt"
	"github.com/rudderlabs/rudder-server/app/cluster"
	"os"
	"sync"

	"github.com/rudderlabs/rudder-server/utils/types/servermode"
)

var _ cluster.ModeProvider = &EnvProvider{}

type EnvProvider struct {
	Mode   servermode.Mode
	ch     chan servermode.Ack
	chOnce sync.Once
}

func (e *EnvProvider) ServerMode() (<-chan servermode.Ack, error) {
	e.chOnce.Do(func() {
		e.ch = make(chan servermode.Ack, 1)
	})
	serverMode := os.Getenv("RSERVER_MODE")
	e.setMode(serverMode)
	e.ch <- servermode.WithACK(servermode.Mode(e.Mode), func() {})
	return e.ch, nil
}

func (e *EnvProvider) Close() {
	if e.ch != nil {
		close(e.ch)
	}
}

func (e *EnvProvider) setMode(mode string) {
	switch mode {
	case "normal":
		e.Mode = servermode.NormalMode
	case "degraded":
		e.Mode = servermode.DegradedMode
	default:
		fmt.Println("Invalid value of RSERVER_MODE")
	}
}

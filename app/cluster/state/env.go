package state

import (
	"fmt"
	"os"

	"github.com/rudderlabs/rudder-server/utils/types/servermode"
)

type Env struct {
	Mode servermode.Mode
}

func (e *Env) ServerMode() <-chan servermode.Ack {
	ch := make(chan servermode.Ack, 1)
	serverMode := os.Getenv("RSERVER_MODE")
	e.setMode(serverMode)
	ch <- servermode.WithACK(servermode.Mode(e.Mode), func() {})
	close(ch)
	return ch
}

func (e *Env) setMode(mode string) {
	switch mode {
	case "normal":
		e.Mode = servermode.NormalMode
	case "degraded":
		e.Mode = servermode.DegradedMode
	default:
		fmt.Println("Invalid value of RSERVER_MODE")
	}
}

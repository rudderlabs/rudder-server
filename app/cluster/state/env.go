package state

import "github.com/rudderlabs/rudder-server/utils/types/servermode"

type Env struct {
}

func (e *Env) ServerMode() <-chan servermode.ModeAck {

}

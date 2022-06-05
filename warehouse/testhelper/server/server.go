package server

import (
	"context"
	r "github.com/rudderlabs/rudder-server/cmd/run"
)

func StartServer() (context.CancelFunc, chan struct{}) {
	svcCtx, svcCancel := context.WithCancel(context.Background())
	svcDone := make(chan struct{})
	go func() {
		r.Run(svcCtx)
		close(svcDone)
	}()
	return svcCancel, svcDone
}

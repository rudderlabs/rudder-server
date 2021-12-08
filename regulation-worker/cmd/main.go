package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/client"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete"
	destination "github.com/rudderlabs/rudder-server/regulation-worker/internal/destination"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/service"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)

		<-c
		cancel()
		close(c)
	}()

	Run(ctx)

}

func Run(ctx context.Context) {
	svc := service.JobSvc{
		API: &client.JobAPI{
			WorkspaceID: config.GetEnv("workspaceID", "1001"),
			URLPrefix:   config.GetEnv("urlPrefix", "http://localhost:35359"),
		},
		DestDetail: &destination.DestMiddleware{
			Dest: &backendconfig.WorkspaceConfig{},
		},
		Deleter: &delete.DeleteRouter{},
	}

	l := withLoop(svc)
	err := l.Loop(ctx)
	if err != nil {
		panic(err)
	}

}

func withLoop(svc service.JobSvc) *service.Looper {

	return &service.Looper{
		Svc: svc,
	}
}

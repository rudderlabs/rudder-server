package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/client"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/service"
)

//TODO: in case of os.Interrupt, syscall.SIGTERM make sure that all are notified about the termination of the service (like transformer, if a job is in progress, etc.)
//creates `loop` object and call getJobLoop and updateStatusLoop methods in different go routines.
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
			WorkspaceID: getWorkspaceID("workspaceID","1001"),
		},
	}

	l := withLoop(svc)
	err := l.Loop(ctx)
	if err != nil {
		panic(err)
	}

}


func getWorkspaceID(name, defaultValue string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return defaultValue
}

func withLoop(svc service.JobSvc) *service.Looper {
	return &service.Looper{Svc: svc}
}

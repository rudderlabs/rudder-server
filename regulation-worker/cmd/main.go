package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/client"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/destination"
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
	defer Cleanup()
	svc := service.JobSvc{
		API: &client.JobAPI{
			WorkspaceID: getEnv("workspaceID", "1001"),
			URLPrefix:   getEnv("urlPrefix", "http://localhost:35359"),
		},
		Deleter: &delete.DeleteFacade{},
		DestDetail: &destination.DestMiddleware{
			Dest:    &backendconfig.WorkspaceConfig{},
			DestCat: &destination.DestCategory{},
		},
	}
	l := withLoop(svc)
	err := l.Loop(ctx)
	if err != nil {
		panic(err)
	}

}

func getEnv(name, defaultValue string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return defaultValue
}

func withLoop(svc service.JobSvc) *service.Looper {

	return &service.Looper{
		Svc: svc,
	}
}

//read all in the present directory
//filter those with extension .json or .json.gz and delete each of them.
func Cleanup() {
	files, err := os.ReadDir(".")
	if err != nil {
		fmt.Println("error while cleanup: %w", err)
	}
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".json" || filepath.Ext(f.Name()) == ".gz" || filepath.Ext(f.Name()) == ".txt" {
			err := os.Remove(f.Name())
			if err != nil {
				fmt.Println("error while deleting file during cleanup: %w", err)
			}
		}
	}
}

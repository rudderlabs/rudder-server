package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/client"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/api"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/batch"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/kvstore"
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
	transformerURL := config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090")
	apiManager := api.APIManager{
		Client:           &http.Client{},
		DestTransformURL: transformerURL,
	}
	router := delete.NewRouter(&kvstore.KVDeleteManager{}, &batch.BatchManager{}, &apiManager)
	urlPrefix, workspaceToken, workspaceId := getServerDetails()
	svc := service.JobSvc{
		API: &client.JobAPI{
			URLPrefix:      urlPrefix,
			WorkspaceToken: workspaceToken,
			WorkspaceID:    workspaceId,
		},
		DestDetail: &destination.DestMiddleware{
			Dest: &backendconfig.WorkspaceConfig{},
		},
		Deleter: router,
	}
	l := withLoop(svc)
	err := l.Loop(ctx)
	if err != nil {
		panic(err)
	}

}

func getServerDetails() (string, string, string) {
	urlPrefix := config.GetEnv("url_prefix", "")
	if urlPrefix == "" {
		panic("regulation-manager URL prefix not found")
	}
	workspaceToken := config.GetEnv("CONFIG_BACKEND_TOKEN", "")
	if workspaceToken == "" {
		panic("workspaceToken not found")
	}
	workspaceId := config.GetEnv("workspaceID", "")
	if workspaceId == "" {
		panic("workspaceId not found")
	}
	return urlPrefix, workspaceToken, workspaceId
}

func withLoop(svc service.JobSvc) *service.Looper {

	return &service.Looper{
		Svc: svc,
	}
}

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/rudderlabs/rudder-server/cmd/suppressionsBackupService/fullExport"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"golang.org/x/sync/errgroup"
)

// setup 2 syncs
// 1. full sync of all suppressions for the namespace/workspace
// 2. partial sync of all suppressions for the namespace/workspace(last 30 days)
// backup both files and serve them via http
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	log := logger.NewLogger().Child("suppressionsBackupService")
	defer cancel()
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)

		<-c
		cancel()
		close(c)
	}()
	if err := backendconfig.Setup(nil); err != nil {
		panic(fmt.Errorf("error while setting up backend config: %v", err))
	}

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return fullExport.SetupSync(ctx, backendconfig.DefaultBackendConfig)
	})

	if err := g.Wait(); err != nil {
		log.Errorf("error: %w: ", err)
	}
}

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/admin"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/suppression-backup-service/api"
	"github.com/rudderlabs/rudder-server/suppression-backup-service/exporter"
	"github.com/rudderlabs/rudder-server/suppression-backup-service/model"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var pkgLogger = logger.NewLogger().Child("suppression-backup-service")

func main() {
	pkgLogger.Info("Starting suppression backup service")
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := Run(ctx)
	if ctx.Err() == nil {
		cancel()
	}
	if err != nil {
		pkgLogger.Error(err)
		os.Exit(1)
	}
}

func Run(ctx context.Context) error {
	err := stats.Default.Start(ctx, rruntime.GoRoutineFactory)
	if err != nil {
		return fmt.Errorf("could not start stats: %w", err)
	}
	defer stats.Default.Stop()

	id, err := getIdentity(ctx)
	if err != nil {
		return fmt.Errorf("could not get identity: %w", err)
	}
	fullExportBaseDir, err := exportPath()
	if err != nil {
		return fmt.Errorf("could not get export path: %w", err)
	}
	latestExportBaseDir, err := exportPath()
	if err != nil {
		return fmt.Errorf("could not get export path: %w", err)
	}

	fullExportFile := model.File{Path: path.Join(fullExportBaseDir, "full-export"), Mu: &sync.RWMutex{}}
	latestExportFile := model.File{Path: path.Join(latestExportBaseDir, "latest-export"), Mu: &sync.RWMutex{}}

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		fullExporter := exporter.Exporter{
			Id:   id,
			File: fullExportFile,
			Log:  pkgLogger,
		}
		return fullExporter.FullExporterLoop(gCtx)
	})

	g.Go(func() error {
		latestExporter := exporter.Exporter{
			Id:   id,
			File: latestExportFile,
			Log:  pkgLogger,
		}
		return latestExporter.LatestExporterLoop(gCtx)
	})

	var httpServer *http.Server
	g.Go(func() error {
		api := api.NewAPI(pkgLogger, fullExportFile, latestExportFile)
		httpServer = &http.Server{
			Addr:              fmt.Sprintf(":%s", config.GetString("HTTP_PORT", "8000")),
			Handler:           api.Handler(gCtx),
			ReadHeaderTimeout: config.GetDuration("HTTP_READ_HEADER_TIMEOUT", 3, time.Second),
		}
		return httpServer.ListenAndServe()
	})
	pkgLogger.Info("http server setup done")

	g.Go(func() error {
		<-gCtx.Done()
		pkgLogger.Info("context.Done triggered... shutting down httpserver")
		return httpServer.Shutdown(context.Background())
	})

	if err := g.Wait(); err != nil {
		if err == http.ErrServerClosed && ctx.Err() == nil {
			return fmt.Errorf("http server exited unexpectedly with error: %v", err)
		}
		return fmt.Errorf("http server exited unexpectedly with error: %v", err)
	}
	return nil
}

func getIdentity(ctx context.Context) (identity.Identifier, error) {
	config.Set("Diagnostics.enableDiagnostics", false)

	admin.Init()
	misc.Init()
	diagnostics.Init()
	backendconfig.Init()

	if err := backendconfig.Setup(nil); err != nil {
		return &identity.NOOP{}, fmt.Errorf("setting up backend config: %w", err)
	}
	defer backendconfig.DefaultBackendConfig.Stop()

	backendconfig.DefaultBackendConfig.StartWithIDs(context.TODO(), "")
	backendconfig.DefaultBackendConfig.WaitForConfig(ctx)

	id := backendconfig.DefaultBackendConfig.Identity()
	return id, nil
}

// exportPath creates a tmp dir and returns the path to it
func exportPath() (baseDir string, err error) {
	tmpDir, err := misc.CreateTMPDIR()
	if err != nil {
		return "", fmt.Errorf("could not create tmp dir: %w", err)
	}
	baseDir = path.Join(tmpDir, "exportV2")
	if err := os.MkdirAll(baseDir, 0o700); err != nil {
		return "", fmt.Errorf("could not create tmp dir: %w", err)
	}
	return
}

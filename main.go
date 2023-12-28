package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/mem"
	"github.com/rudderlabs/rudder-server/api"
	"github.com/rudderlabs/rudder-server/runner"
	_ "github.com/rudderlabs/rudder-server/swagger/docs"
	_ "go.uber.org/automaxprocs"
)

var (
	version                                                                  = "Not an official release. Get the latest release from the github repo."
	major, minor, commit, buildDate, builtBy, gitURL, patch, enterpriseToken string
)

func main() {

	// Serve Swagger UI
	fs := http.FileServer(http.Dir("/github.com/rudderlabs/rudder-server/swaggerui"))
	http.Handle("/swaggerui/", http.StripPrefix("/swaggerui/", fs))

	// Start serving static files in the background
	go func() {
		if err := http.ListenAndServe(":8080", nil); err != nil {
			logger.NewLogger().Errorw("Failed to start static file server", "error", err)
		}
	}()

	e := echo.New()

	// Middleware
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.BasicAuth(func(username, password string, c echo.Context) (bool, error) {
		// Basic authentication logic
		if username == "your_write_key" && password == "" {
			return true, nil
		}
		return false, nil
	}))

	// Routes for Identify, Track, Page, Screen, Group, Alias, and Batch
	e.POST("/v1/identify", api.IdentifyHandler)
	e.POST("/v1/track", api.TrackHandler)
	e.POST("/v1/page", api.PageHandler)
	e.POST("/v1/screen", api.ScreenHandler)
	e.POST("/v1/group", api.GroupHandler)
	e.POST("/v1/alias", api.AliasHandler)
	e.POST("/v1/batch", api.BatchHandler)

	// Start Echo server
	go func() {
		if err := e.Start(":3000"); err != nil {
			logger.NewLogger().Fatalw("Echo server failed to start", "error", err)
		}
	}()

	if memStat, err := mem.Get(); err == nil {
		memoryLimit := int64(80 * memStat.Total / 100)
		logger.NewLogger().Infow("Setting memory limit to", "limit", memoryLimit)
		debug.SetMemoryLimit(memoryLimit)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	r := runner.New(runner.ReleaseInfo{
		Version:         version,
		Major:           major,
		Minor:           minor,
		Patch:           patch,
		Commit:          commit,
		BuildDate:       buildDate,
		BuiltBy:         builtBy,
		GitURL:          gitURL,
		EnterpriseToken: config.GetString("ENTERPRISE_TOKEN", enterpriseToken),
	})
	exitCode := r.Run(ctx, os.Args)
	cancel()
	os.Exit(exitCode)
}

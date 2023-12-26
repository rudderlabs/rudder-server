package main

import (
	"context"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	_ "go.uber.org/automaxprocs"

	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"github.com/pdrum/swagger-automation/api"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/mem"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/swagger/api"
	_ "github.com/rudderlabs/rudder-server/swagger/docs"
)

var (
	version                                                                  = "Not an official release. Get the latest release from the github repo."
	major, minor, commit, buildDate, builtBy, gitURL, patch, enterpriseToken string
)

func main() {
	e := echo.New()

	// Middleware
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.BasicAuth(func(username, password string, c echo.Context) (bool, error) {

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

	// Start server
	e.Logger.Fatal(e.Start(":1323"))

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

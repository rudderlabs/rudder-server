package main

import (
	"os"
	"runtime/debug"
	"syscall"
	"time"

	_ "go.uber.org/automaxprocs"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/mem"

	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/utils/signal"
)

var (
	version                                     = "Not an official release. Get the latest release from the github repo."
	commit, buildDate, builtBy, enterpriseToken string
)

func main() {
	c := config.Default
	log := logger.NewLogger().Child("main")
	ctx, cancel := signal.NotifyContextWithCallback(func() {
		log.Infon("Server received termination signal...")
	}, syscall.SIGINT, syscall.SIGTERM)

	log.Infon("Server is starting up...")
	start := time.Now()

	if memStat, err := mem.Get(); err == nil {
		memoryLimit := int64(80 * memStat.Total / 100)
		log.Infon("Setting memory limit", logger.NewIntField("limit", memoryLimit))
		debug.SetMemoryLimit(memoryLimit)
	}

	shutdownOnNonReloadableConfigChange := c.GetReloadableBoolVar(false, "shutdownOnNonReloadableConfigChange")
	c.OnNonReloadableConfigChange(func(key string) {
		switch key {
		case "statsExcludedTags": // keys to ignore
			// no-op
		default:
			if shutdownOnNonReloadableConfigChange.Load() {
				log.Infon("Config change detected, shutting down server...", logger.NewStringField("key", key))
				cancel()
			} else {
				log.Infon("Config change detected, but server will not shut down", logger.NewStringField("key", key))
			}
		}
	})

	r := runner.New(runner.ReleaseInfo{
		Version:         version,
		Commit:          commit,
		BuildDate:       buildDate,
		BuiltBy:         builtBy,
		EnterpriseToken: config.GetString("ENTERPRISE_TOKEN", enterpriseToken),
	})
	exitCode := r.Run(ctx, os.Args)

	log.Infon("Server was shut down",
		logger.NewIntField("exitCode", int64(exitCode)),
		logger.NewDurationField("uptime", time.Since(start)),
	)
	cancel()
	os.Exit(exitCode)
}

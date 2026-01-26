package main

import (
	"os"
	"sync"
	"syscall"
	"time"

	_ "go.uber.org/automaxprocs"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/mem"

	kitctx "github.com/rudderlabs/rudder-go-kit/context"
	"github.com/rudderlabs/rudder-server/runner"
)

var (
	version                                     = "Not an official release. Get the latest release from the github repo."
	commit, buildDate, builtBy, enterpriseToken string
)

func main() {
	c := config.Default
	log := logger.NewLogger().Child("main")
	ctx, shutdownFn := kitctx.NotifyContextWithCallback(func() {
		log.Infon("Server received termination signal...")
	}, syscall.SIGINT, syscall.SIGTERM)

	log.Infon("Server is starting up...")
	start := time.Now()

	var wg sync.WaitGroup
	mem.WatchMemoryLimit(ctx, &wg,
		mem.WatchWithLogger(log),
		mem.WatchWithPercentageLoader(c.GetReloadableIntVar(90, 1, "memoryLimitPercentage")),
	)

	shutdownOnNonReloadableConfigChange := c.GetReloadableBoolVar(false, "shutdownOnNonReloadableConfigChange")
	c.OnNonReloadableConfigChange(func(key string) {
		switch key {
		case "statsExcludedTags": // keys to ignore
			// no-op
		default:
			if shutdownOnNonReloadableConfigChange.Load() {
				log.Infon("Config change detected, shutting down server...", logger.NewStringField("key", key))
				shutdownFn()
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
	exitCode := r.Run(ctx, shutdownFn, os.Args)

	log.Infon("Server was shut down",
		logger.NewIntField("exitCode", int64(exitCode)),
		logger.NewDurationField("uptime", time.Since(start)),
	)
	shutdownFn()
	wg.Wait()
	os.Exit(exitCode)
}

package main

import (
	"context"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	_ "go.uber.org/automaxprocs"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/mem"

	"github.com/rudderlabs/rudder-server/runner"
)

var (
	version                                     = "Not an official release. Get the latest release from the github repo."
	commit, buildDate, builtBy, enterpriseToken string
)

func main() {
	if memStat, err := mem.Get(); err == nil {
		memoryLimit := int64(80 * memStat.Total / 100)
		logger.NewLogger().Infow("Setting memory limit to", "limit", memoryLimit)
		debug.SetMemoryLimit(memoryLimit)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	r := runner.New(runner.ReleaseInfo{
		Version:         version,
		Commit:          commit,
		BuildDate:       buildDate,
		BuiltBy:         builtBy,
		EnterpriseToken: config.GetString("ENTERPRISE_TOKEN", enterpriseToken),
	})
	exitCode := r.Run(ctx, os.Args)
	cancel()
	os.Exit(exitCode)
}

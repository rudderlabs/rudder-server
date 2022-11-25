package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	_ "go.uber.org/automaxprocs"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/runner"
)

var (
	version                                                                  = "Not an official release. Get the latest release from the github repo."
	major, minor, commit, buildDate, builtBy, gitURL, patch, enterpriseToken string
)

func main() {
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

package main_test

import (
	"context"
	"os"
	"testing"

	main "github.com/rudderlabs/rudder-server/regulation-worker/cmd"
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	svcCtx, svcCancel := context.WithCancel(context.Background())
	workspaceID := "1001"
	urlPrefix := "http://localhost:35359"
	go func() {
		_ = os.Setenv("workspaceID", workspaceID)
		_ = os.Setenv("urlPrefix", urlPrefix)
		main.Run(svcCtx)
	}()

	code := m.Run()

	svcCancel()

	return code
}

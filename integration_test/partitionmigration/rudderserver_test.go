package partitionmigration_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"

	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
)

// buildRudderServerBinary builds the rudder-server binary and returns its path.
func buildRudderServerBinary(t *testing.T, binaryPath string) {
	name := "testbinary"
	buildCmd := exec.Command("go", "build", "-o", binaryPath, "../../main.go")
	buildCmd.Stderr = os.Stderr
	buildCmd.Stdout = os.Stdout
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("failed to build rudder-server for %s: %v", name, err)
	}
}

// startRudderServer starts a rudder-server process with the given environment configuration in a separate goroutine managed by the provided errgroup.Group.
func startRudderServer(ctx context.Context, g *errgroup.Group, name, binaryPath string, configs map[string]string) {
	g.Go(func() error {
		cmd := exec.CommandContext(ctx, binaryPath)
		cmd.Env = append(os.Environ(), lo.MapToSlice(configs, func(k, v string) string {
			return config.ConfigKeyToEnv(config.DefaultEnvPrefix, k) + "=" + v
		})...)
		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("server %q: failed to start: %w", name, err)
		}
		if err := cmd.Wait(); err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
					if status.Signaled() {
						return nil
					}
				}
			}
			return fmt.Errorf("server %q exited with error: %w", name, err)
		}
		return nil
	})
}

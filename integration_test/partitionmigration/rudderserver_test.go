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
	buildCmd := exec.Command("go", "build", "-cover", "-o", binaryPath, "../../main.go")
	buildCmd.Stderr = os.Stderr
	buildCmd.Stdout = os.Stdout
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("failed to build rudder-server for %s: %v", name, err)
	}
}

// startRudderServer starts a rudder-server process with the given environment configuration in a separate goroutine managed by the provided errgroup.Group.
// It sends SIGTERM when the context is cancelled to allow graceful shutdown and proper coverage data collection.
func startRudderServer(t *testing.T, ctx context.Context, g *errgroup.Group, name, binaryPath string, configs map[string]string) {
	coverDir := t.TempDir()
	configs["GOCOVERDIR"] = coverDir
	// Don't use exec.CommandContext - it sends SIGKILL on context cancellation,
	// which prevents the process from writing coverage data.
	cmd := exec.Command(binaryPath)
	cmd.Env = append(os.Environ(), lo.MapToSlice(configs, func(k, v string) string {
		return config.ConfigKeyToEnv(config.DefaultEnvPrefix, k) + "=" + v
	})...)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	if err := cmd.Start(); err != nil {
		t.Fatalf("server %q: failed to start: %v", name, err)
	}

	g.Go(func() error {
		defer convertCoverageData(t, coverDir, name+"-profile.out")

		// Wait for context cancellation, then send SIGTERM for graceful shutdown
		<-ctx.Done()
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Logf("server %q: failed to send SIGTERM: %v", name, err)
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

func convertCoverageData(t *testing.T, coverDir, outputFile string) {
	cmd := exec.Command("go", "tool", "covdata", "textfmt", "-i="+coverDir, "-o="+outputFile)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Logf("failed to convert coverage data: %v", err)
	}
}

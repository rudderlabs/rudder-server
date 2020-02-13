package rruntime

/*
Package rruntime stands for rudder runtime
This package functions include:
- Handling terminate signals from underlying infrastructure
- Go routines used by the program to function in a certain way like panic handling etc.
*/

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/rudderlabs/rudder-server/utils/monitoring"

	"github.com/rudderlabs/rudder-server/utils/logger"
)

// IsShutDownInProgess flag is set to true when the process receives a SIGINT/SIGTERM signal
// Other goroutines are supposed to use this flag and terminate themselves
// to ensure graceful shutdown of the server
var IsShutDownInProgess bool = false

// Subscribes to os terminate signals and start listener go routine for handling signals
func init() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go handleTerminateSignal(sigs)
	fmt.Println("subscribed to terminate signals")
}

func handleTerminateSignal(signalChannel <-chan os.Signal) {
	sig := <-signalChannel
	logger.Infof("Received terminate signal %v. Server entering shutting down state.", sig)
	monitoring.StopCPUProfiling()
	monitoring.StopMemoryProfiling()
	// clearing zap Log buffer to std output
	if logger.Log != nil {
		logger.Log.Sync()
	}
	IsShutDownInProgess = true
	// os.Exit(1)
}

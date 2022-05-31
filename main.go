package main

import (
	"context"
	"github.com/rudderlabs/rudder-server/cmd/run"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		cancel()
	}()

	Run(ctx)
}

func Run(ctx context.Context) {
	run.Run(ctx)
}

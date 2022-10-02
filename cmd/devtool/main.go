package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/rudderlabs/rudder-server/cmd/devtool/commands"
	"github.com/urfave/cli/v2"
)

func main() {
	_ = godotenv.Load(".env")

	app := &cli.App{
		Name:                 "devtool",
		Usage:                "a collection of commands to help with development",
		Flags:                []cli.Flag{},
		Commands:             commands.DefaultList,
		EnableBashCompletion: true,
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.CommandsByName(app.Commands))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	err := app.RunContext(ctx, os.Args)
	if err != nil {
		fmt.Println("Fail to run:", err)
	}
}

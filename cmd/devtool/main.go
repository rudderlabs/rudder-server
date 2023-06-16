package main

import (
	"fmt"
	"os"
	"sort"

	"github.com/joho/godotenv"
	"github.com/urfave/cli/v2"

	"github.com/rudderlabs/rudder-server/cmd/devtool/commands"
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

	err := app.Run(os.Args)
	if err != nil {
		fmt.Println("Fail to run:", err)
	}
}

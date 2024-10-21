package main

import (
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/urfave/cli/v2"

	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/client"
	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/warehouse"
)

func main() {
	app := cli.NewApp()
	app.Name = "rudder"
	app.Version = "0.1.1"
	app.Description = "A command line interface to your Rudder"

	app.Commands = []*cli.Command{
		{
			Name:  "trigger-wh-upload",
			Usage: "Start warehouse uploads without any delay irrepective of configured sync schedule",
			Flags: []cli.Flag{
				&cli.BoolFlag{
					Name:  "off, o",
					Usage: `Use this to turn off explicit warehouse upload triggers. Warehouse uploads will continue to be done as per schedule in control plane.`,
				},
			},
			Action: func(c *cli.Context) error {
				var reply string
				err := client.GetUDSClient().Call("Warehouse.TriggerUpload", c.Bool("off"), &reply)
				fmt.Println(reply)
				return err
			},
		},
		{
			Name:  "wh-query",
			Usage: "Query underlying warehouse",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    "dest",
					Usage:   `Specify destination ID to query underlying warehouse`,
					Aliases: []string{"d"},
				},
				&cli.StringFlag{
					Name:    "source",
					Usage:   `Specify source ID to query underlying warehouse`,
					Aliases: []string{"src"},
				},
				&cli.StringFlag{
					Name:    "sql",
					Usage:   `Specify SQL statement to query underlying warehouse`,
					Aliases: []string{"s"},
				},
				&cli.StringFlag{
					Name:    "file",
					Usage:   `Specify SQL file to query underlying warehouse`,
					Aliases: []string{"f"},
				},
			},
			Action: func(c *cli.Context) error {
				err := warehouse.Query(c)
				return err
			},
		},
		{
			Name:  "wh-test",
			Usage: "Test underlying warehouse",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    "dest",
					Usage:   `Specify destination ID to test underlying warehouse`,
					Aliases: []string{"d"},
				},
			},
			Action: func(c *cli.Context) error {
				err := warehouse.ConfigurationTest(c)
				return err
			},
		},
		{
			Name:  "logging",
			Usage: "Set log level for module. It will affect the module and it's children",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name: "module",
					Usage: `Specify module to set log level examples: router, router.GA
		Use "root" to set server wide logging level`,
					Aliases: []string{"m"},
				},
				&cli.StringFlag{
					Name:    "level",
					Usage:   `Valid log levels are EVENT, DEBUG, INFO, WARN, ERROR, FATAL`,
					Aliases: []string{"l"},
				},
			},
			Action: func(c *cli.Context) error {
				var reply string
				module := c.String("module")
				if module == "root" {
					module = ""
				}
				err := client.GetUDSClient().Call("Admin.SetLogLevel",
					struct {
						Module string
						Level  string
					}{module, c.String("level")}, &reply)
				fmt.Println(reply)
				return err
			},
		},
		{
			Name:  "logging-config",
			Usage: "Gets Logging Configuration",
			Action: func(c *cli.Context) error {
				var reply string
				var noArgs struct{}
				err := client.GetUDSClient().Call("Admin.GetLoggingConfig", noArgs, &reply)
				if err == nil {
					fmt.Println(reply)
				}
				return err
			},
		},
	}

	// Global Flags
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "server-dir",
			Usage:   "Set your Server Directory, Default is ",
			EnvVars: []string{"config.RudderServerPathKey"},
			Aliases: []string{"s"},
		},
		&cli.StringFlag{
			Name:    "env-file",
			Usage:   "Set your Env file name, Default is ",
			EnvVars: []string{"config.RudderEnvFilePathKey"},
			Aliases: []string{"e"},
		},
	}
	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.CommandsByName(app.Commands))
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

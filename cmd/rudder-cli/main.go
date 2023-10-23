package main

import (
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/manifoldco/promptui"
	"github.com/tidwall/gjson"
	"github.com/urfave/cli/v2"

	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/client"
	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/config"
	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/db"
	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/gateway"
	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/router"
	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/stash"
	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/status"
	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/warehouse"
)

func main() {
	app := cli.NewApp()
	app.Name = "rudder"
	app.Version = "0.1.1"
	app.Description = "A command line interface to your Rudder"

	app.Commands = []cli.Command{
		{
			Name:  "status",
			Usage: "Display status of depending services",
			// Flags: StatusFlags(),
			Action: func(c *cli.Context) error {
				status.DisplayStatus()
				return nil
			},
		},
		{
			Name:  "sqlquery",
			Usage: "Display status of SQL Query",
			Action: func(c *cli.Context) error {
				prompt := promptui.Select{
					Label: "Select Generic SQL Query",
					Items: []string{"Jobs between JobID's of a User", "Error Code Count By Destination"},
				}
				_, result, err := prompt.Run()
				if err != nil {
					return err
				}
				err = status.GetArgs(result)
				return err
			},
		},
		{
			Name:  "server-status",
			Usage: "Shows rudder server status.",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name: "jsonpath",
					Usage: `Json path to filter output, such as \"gateway.ack-count\" or \"server-mode\".
	  	Refer https://github.com/tidwall/gjson/blob/master/SYNTAX.md for more info
					`,
					Aliases: []string{"j"},
				},
			},
			Action: func(c *cli.Context) error {
				var reply string
				var noArgs struct{}
				err := client.GetUDSClient().Call("Admin.Status", noArgs, &reply)
				if err == nil && c.String("jsonpath") != "" {
					reply = gjson.Get(reply, c.String("jsonpath")).String()
				}
				fmt.Println(reply)
				return err
			},
		},
		{
			Name:  "server-config",
			Usage: "Gets server configuration",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name: "jsonpath",
					Usage: `Json path to filter output, such as \"gateway.ack-count\" or \"server-mode\".
	  	Refer https://github.com/tidwall/gjson/blob/master/SYNTAX.md for more info
					`,
					Aliases: []string{"j"},
				},
			},
			Action: func(c *cli.Context) error {
				var reply string
				var noArgs struct{}
				err := client.GetUDSClient().Call("Admin.ServerConfig", noArgs, &reply)
				if err == nil && c.String("jsonpath") != "" {
					reply = gjson.Get(reply, c.String("jsonpath")).String()
				}
				fmt.Println(reply)
				return err
			},
		},
		{
			Name:  "routing-config",
			Usage: "Gets current routing configuration from the server",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name: "jsonpath",
					Usage: `Json path to filter output, such as \"gateway.ack-count\" or \"server-mode\".
	  	Refer https://github.com/tidwall/gjson/blob/master/SYNTAX.md for more info
					`,
					Aliases: []string{"j"},
				},
				&cli.BoolFlag{
					Name:  "only-processor-enabled",
					Usage: "Pass this flag with value true to filter config for processor enabled destinations",
				},
			},
			Action: func(c *cli.Context) error {
				var reply string
				err := client.GetUDSClient().Call("BackendConfig.RoutingConfig", c.Bool("only-processor-enabled"), &reply)
				if err == nil && c.String("jsonpath") != "" {
					reply = gjson.Get(reply, c.String("jsonpath")).String()
				}
				fmt.Println(reply)
				return err
			},
		},
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
			Name:  "config-env",
			Usage: "Get formatted env key",
			Action: func(c *cli.Context) error {
				if c.NArg() != 1 {
					fmt.Println("config-env takes only single argument. Ex:router.GA.enabled")
					return nil
				}
				argument := c.Args().Get(0)
				var reply string
				err := client.GetUDSClient().Call("Admin.GetFormattedEnv", argument, &reply)
				if err == nil {
					fmt.Println(reply)
				}
				return err
			},
		},
		{
			Name:  "ds-status",
			Usage: "Get broader level ds table status",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    "type",
					Usage:   `Specify ds type, one of rt, brt, gw, proc_error`,
					Aliases: []string{"t"},
				},
				&cli.StringFlag{
					Name:    "dsnum",
					Usage:   `Specify the ds table num`,
					Aliases: []string{"d"},
				},
				&cli.StringFlag{
					Name:    "format",
					Usage:   `Specify output format, json for json string, table for formatted table`,
					Aliases: []string{"f"},
				},
			},
			Action: func(c *cli.Context) error {
				var err error
				if c.String("type") == "gw" {
					err = gateway.Query(c)
				}
				if c.String("type") == "proc_error" {
					err = stash.Query(c)
				}
				if c.String("type") == "rt" {
					err = router.Query(c, "Router")
				}
				if c.String("type") == "brt" {
					err = router.Query(c, "BatchRouter")
				}
				return err
			},
		},
		{
			Name:  "ds-list",
			Usage: "Get List of DS Tables",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    "type",
					Usage:   `Specify ds type, one of rt, brt, gw, proc_error`,
					Aliases: []string{"t"},
				},
			},
			Action: func(c *cli.Context) error {
				err := db.QueryDS(c, c.String("type"))
				return err
			},
		},
		{
			Name:  "job-by-id",
			Usage: "Get Job by ID",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    "type",
					Usage:   `Specify ds type, one of rt, brt, gw, proc_error`,
					Aliases: []string{"t"},
				},
				&cli.StringFlag{
					Name:    "jobid",
					Usage:   `Specify the jobid`,
					Aliases: []string{"j"},
				},
			},
			Action: func(c *cli.Context) error {
				err := db.QueryJobByID(c, c.String("type"))
				return err
			},
		},
		{
			Name:  "jobs-status",
			Usage: "Get Statuses of Jobs in DS Tables grouped by SrcId, DestID, CustomVal",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    "type",
					Usage:   `Specify ds type, one of rt, brt, gw, proc_error`,
					Aliases: []string{"t"},
				},
				&cli.StringFlag{
					Name:    "dsnum",
					Usage:   `Specify the ds table num`,
					Aliases: []string{"d"},
				},
				&cli.StringFlag{
					Name:    "numds",
					Usage:   `Number of Jobs in the first n DS`,
					Aliases: []string{"n"},
				},
				&cli.StringFlag{
					Name:    "format",
					Usage:   `Specify output format, json for json string, table for formatted table`,
					Aliases: []string{"f"},
				},
			},
			Action: func(c *cli.Context) error {
				err := db.QueryCount(c, c.String("type"))
				return err
			},
		},
		{
			Name:  "get-failed",
			Usage: "Get Last 5 Failed Jobs in DS Tables",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    "type",
					Usage:   `Specify ds type, one of rt, brt, gw, proc_error`,
					Aliases: []string{"t"},
				},
				&cli.StringFlag{
					Name:    "dsnum",
					Usage:   `Specify the ds table num`,
					Aliases: []string{"d"},
				},
				&cli.StringFlag{
					Name:    "destType",
					Usage:   `Specify the destination type`,
					Aliases: []string{"dt"},
				},
			},
			Action: func(c *cli.Context) error {
				err := db.QueryFailedJob(c, c.String("type"))
				return err
			},
		},
		{
			Name:  "job-status",
			Usage: "Get Statuses of the given Job ID",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:    "type",
					Usage:   `Specify ds type, one of rt, brt, gw, proc_error`,
					Aliases: []string{"t"},
				},
				cli.StringFlag{
					Name:    "jobid",
					Usage:   `Specify the jobid`,
					Aliases: []string{"j"},
				},
				cli.StringFlag{
					Name:    "format",
					Usage:   `Specify output format, json for json string, table for formatted table`,
					Aliases: []string{"f"},
				},
			},
			Action: func(c *cli.Context) error {
				err := db.QueryJobIDStatus(c, c.String("type"))
				return err
			},
		},
		{
			Name:  "logging",
			Usage: "Set log level for module. It will affect the module and it's children",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "module",
					Usage: `Specify module to set log level examples: router, router.GA
		Use "root" to set server wide logging level`,
					Aliases: []string{"m"},
				},
				cli.StringFlag{
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

	app.Before = func(c *cli.Context) error {
		if c.GlobalString("server-dir") != "" {
			os.Setenv(config.RudderEnvFilePathKey, c.GlobalString("server-dir"))
		}
		if c.GlobalString("env-file") != "" {
			os.Setenv(config.RudderEnvFilePathKey, c.GlobalString("env-file"))
		}
		return nil
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.CommandsByName(app.Commands))
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

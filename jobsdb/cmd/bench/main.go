package main

import (
	"context"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/urfave/cli/v2"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

func main() {
	tool := cli.NewApp()
	tool.Name = "jobsdbBench"
	tool.Description = "A command line benchmark tool for jobsdb"
	log := logger.NewLogger().Child("jobsdb")
	conf := config.New(config.WithEnvPrefix("jobsdb"))
	b := &Bencher{
		conf: conf,
		log:  log,
	}
	var (
		// base test

		numReaders             int
		numWriters             int
		clearDB                bool
		numJobsPerReader       int
		eventSize              int
		failurePercentage      int
		eventsPickedUpPerQuery int
		verbose                bool

		// more vars for other cases below
	)

	tool.Commands = []*cli.Command{
		{
			Name:        "base",
			Usage:       "run a benchmark with m readers and n writers concurrently",
			Description: "creates jobs of m sourceIDs for the writers to query. Readers query by sourceID",
			Flags: []cli.Flag{
				&cli.IntFlag{
					Name:        "numReaders",
					Usage:       "number of concurrent readers - test will create as many sourceIDs",
					Aliases:     []string{"m"},
					Destination: &numReaders,
					DefaultText: `config.GetInt("numReaders", 10)`,
				},
				&cli.IntFlag{
					Name:        "numWriters",
					Usage:       "number of concurrent writers",
					Aliases:     []string{"n"},
					Destination: &numWriters,
					DefaultText: `config.GetInt("numWriters", 10)`,
				},
				&cli.IntFlag{
					Name:        "numJobsPerReader",
					Usage:       "number of jobs per sourceID/reader",
					Aliases:     []string{"j"},
					Destination: &numJobsPerReader,
					DefaultText: `config.GetInt("numJobsPerReader", 10000)`,
				},
				&cli.IntFlag{
					Name:        "eventSize",
					Usage:       "payload size of the events in bytes(estimate)",
					Aliases:     []string{"e"},
					Destination: &eventSize,
					DefaultText: `config.GetInt("eventSize", 1536)`,
				},
				&cli.IntFlag{
					Name:        "failurePercentage",
					Usage:       "percentage of events that fail on the first attempt, all pass on second attempt",
					Aliases:     []string{"f"},
					Destination: &failurePercentage,
					DefaultText: `0`,
				},
				&cli.IntFlag{
					Name:        "eventsPickedUpPerQuery",
					Usage:       "number of events picked up per query",
					Aliases:     []string{"q"},
					Destination: &eventsPickedUpPerQuery,
					DefaultText: `config.GetInt("eventsPickedUpPerQuery", 2000)`,
				},
				&cli.BoolFlag{
					Name:        "clearDB",
					Usage:       "cleardb before starting",
					Aliases:     []string{"c"},
					Destination: &clearDB,
					DefaultText: `false`,
				},
				&cli.BoolFlag{
					Name:        "verbose",
					Usage:       "verbose benchmark script logging",
					Aliases:     []string{"v"},
					Destination: &verbose,
					DefaultText: `false`,
				},
			},
			Action: func(ctx *cli.Context) error {
				if verbose {
					logger.SetLogLevel("jobsdb", "DEBUG")
				}
				return b.RunBaseTest(ctx.Context, BaseTest{
					ClearDB:                clearDB,
					FailurePercent:         failurePercentage,
					NumWriters:             numWriters,
					NumReaders:             numReaders,
					NumJobsPerTopic:        numJobsPerReader,
					EventSize:              eventSize,
					EventsPickedUpPerQuery: eventsPickedUpPerQuery,
				})
			},
		},
	}

	sort.Sort(cli.CommandsByName(tool.Commands))
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := tool.RunContext(ctx, os.Args); err != nil {
		log.Fatal(err)
	}
}

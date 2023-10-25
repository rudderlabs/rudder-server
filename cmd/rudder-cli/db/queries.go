package db

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/lensesio/tableprinter"
	_ "github.com/lib/pq"
	"github.com/urfave/cli/v2"

	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/client"
)

type FailedStatusStats struct {
	FailedStatusStats []JobStatusT
}
type EventStatusStats struct {
	StatsNums []EventStatusDetailed
	DSList    string
}

type EventStatusDetailed struct {
	Status        string `header:"Status"`
	SourceID      string `header:"SourceID"`
	DestinationID string `header:"DestinationID"`
	CustomVal     string `header:"CustomVal"`
	Count         int    `header:"Count"`
}

type JobStatusT struct {
	JobID         int64           `header:"JobID"`
	JobState      string          `header:"JobState"` // ENUM waiting, executing, succeeded, waiting_retry,  failed, aborted, migrating, migrated, wont_migrate
	AttemptNum    int             `header:"AttemptNum"`
	ExecTime      time.Time       `header:"ExecTime"`
	RetryTime     time.Time       `header:"RetryTime"`
	ErrorCode     string          `header:"ErrorCode"`
	ErrorResponse json.RawMessage `header:"ErrorResponse"`
}

func getModuleFromType(dsType string) string {
	if dsType == "gw" {
		return "Gateway"
	}
	if dsType == "proc_error" {
		return "ProcErrors"
	}
	if dsType == "rt" {
		return "Router"
	}
	if dsType == "brt" || dsType == "batch_rt" {
		return "BatchRouter"
	}

	return ""
}

func QueryDS(c *cli.Context, dsType string) error {
	var reply string
	err := client.GetUDSClient().Call(fmt.Sprintf("%s.GetDSList", getModuleFromType(dsType)), "", &reply)
	if err == nil {
		fmt.Println(reply)
	}
	return err
}

func QueryCount(c *cli.Context, dsType string) error {
	var reply string
	kwargs := c.String("dsnum") + ":" + c.String("numds")
	err := client.GetUDSClient().Call(fmt.Sprintf("%s.GetDSJobCount", getModuleFromType(dsType)), kwargs, &reply)
	if err == nil {
		if c.String("format") == "json" {
			fmt.Println(reply)
		} else {
			var response EventStatusStats
			printer := tableprinter.New(os.Stdout)
			printer.BorderTop, printer.BorderBottom, printer.BorderLeft, printer.BorderRight = true, true, true, true
			printer.CenterSeparator = "│"
			printer.ColumnSeparator = "│"
			printer.RowSeparator = "─"
			err = json.Unmarshal([]byte(reply), &response)
			if err == nil {
				fmt.Println("Job Status Summary")
				fmt.Println("================================================================================")
				printer.Print(response.StatsNums)
				fmt.Println("================================================================================")
				fmt.Println("Fetched From")
				fmt.Println(response.DSList)
			}
		}
	}
	return err
}

func QueryFailedJob(c *cli.Context, dsType string) error {
	var reply string
	kwargs := c.String("dsnum") + ":" + c.String("destType")
	err := client.GetUDSClient().Call(fmt.Sprintf("%s.GetDSFailedJobs", getModuleFromType(dsType)), kwargs, &reply)
	if err == nil {
		fmt.Println(reply)
	}
	return err
}

func QueryJobByID(c *cli.Context, dsType string) error {
	var reply string
	kwargs := c.String("jobid")
	err := client.GetUDSClient().Call(fmt.Sprintf("%s.GetJobByID", getModuleFromType(dsType)), kwargs, &reply)
	if err == nil {
		fmt.Println(reply)
	}
	return err
}

func QueryJobIDStatus(c *cli.Context, dsType string) error {
	var reply string
	kwargs := c.String("jobid")
	err := client.GetUDSClient().Call(fmt.Sprintf("%s.GetJobIDStatus", getModuleFromType(dsType)), kwargs, &reply)
	if err == nil {
		if c.String("format") == "json" {
			fmt.Println(reply)
		} else {
			var response FailedStatusStats
			printer := tableprinter.New(os.Stdout)
			printer.BorderTop, printer.BorderBottom, printer.BorderLeft, printer.BorderRight = true, true, true, true
			printer.CenterSeparator = "│"
			printer.ColumnSeparator = "│"
			printer.RowSeparator = "─"
			err = json.Unmarshal([]byte(reply), &response)
			response = formatResponse(response)
			if err == nil {
				fmt.Println("Job Status Summary")
				fmt.Println("================================================================================")
				printer.Print(response.FailedStatusStats)
				fmt.Println("================================================================================")
			}
		}
	}
	return err
}

func formatResponse(response FailedStatusStats) FailedStatusStats {
	var updatedResponse FailedStatusStats
	for _, statusT := range response.FailedStatusStats {
		statusT.ErrorResponse = nil
		updatedResponse.FailedStatusStats = append(updatedResponse.FailedStatusStats, statusT)
	}
	return updatedResponse
}

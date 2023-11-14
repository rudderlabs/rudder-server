package router

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/lensesio/tableprinter"
	"github.com/urfave/cli/v2"

	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/client"
)

type JobCountsByStateAndDestination struct {
	Count       int    `header:"Count"`
	State       string `header:"State"`
	Destination string `header:"Destination"`
}

type ErrorCodeCountsByDestination struct {
	ErrorCode     string `header:"Error Code"`
	Count         int    `header:"Count"`
	Destination   string `header:"Destination"`
	DestinationID string `header:"DestinationID"`
}

type JobCountByConnections struct {
	Count         int    `header:"Count"`
	SourceId      string `header:"SourceID"`
	DestinationId string `header:"DestinationID"`
}

type LatestJobStatusCounts struct {
	Count int    `header:"Count"`
	State string `header:"State"`
	Rank  int    `header:"Rank"`
}

type DSStats struct {
	JobCountsByStateAndDestination []JobCountsByStateAndDestination
	ErrorCodeCountsByDestination   []ErrorCodeCountsByDestination
	JobCountByConnections          []JobCountByConnections
	LatestJobStatusCounts          []LatestJobStatusCounts
	UnprocessedJobCounts           int
}

func Query(c *cli.Context, router string) error {
	var reply string
	err := client.GetUDSClient().Call(router+".GetDSStats", c.String("dsnum"), &reply)
	if err == nil {
		if c.String("format") == "json" {
			fmt.Println(reply)
		} else {
			var response DSStats
			printer := tableprinter.New(os.Stdout)
			printer.BorderTop, printer.BorderBottom, printer.BorderLeft, printer.BorderRight = true, true, true, true
			printer.CenterSeparator = "│"
			printer.ColumnSeparator = "│"
			printer.RowSeparator = "─"
			err = json.Unmarshal([]byte(reply), &response)
			if err == nil {
				fmt.Println("JobCountByConnections")
				fmt.Println("================================================================================")
				printer.Print(response.JobCountByConnections)
				fmt.Println("JobCountsByStateAndDestination")
				fmt.Println("================================================================================")
				printer.Print(response.JobCountsByStateAndDestination)
				fmt.Println("ErrorCodeCountsByDestination")
				fmt.Println("================================================================================")
				printer.Print(response.ErrorCodeCountsByDestination)
				fmt.Println("LatestJobStatusCounts")
				fmt.Println("================================================================================")
				printer.Print(response.LatestJobStatusCounts)
				fmt.Println("================================================================================")
				fmt.Println("UnprocessedJobCounts : ", response.UnprocessedJobCounts)
			}
		}
	}
	return err
}

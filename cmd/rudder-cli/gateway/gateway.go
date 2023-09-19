package gateway

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/lensesio/tableprinter"
	"github.com/urfave/cli"

	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/client"
)

type SourceEvents struct {
	Count int    `header:"Count"`
	Name  string `header:"Name"`
	ID    string `header:"ID"`
}

type DSStats struct {
	SourceNums   []SourceEvents
	NumUsers     int
	AvgBatchSize float64
	TableSize    int64
	NumRows      int
}

func Query(c *cli.Context) error {
	var reply string
	err := client.GetUDSClient().Call("Gateway.GetDSStats", c.String("dsnum"), &reply)
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
				fmt.Println("EventsBySource")
				fmt.Println("================================================================================")
				printer.Print(response.SourceNums)
				fmt.Println("================================================================================")
				fmt.Println("NumUsers : ", response.NumUsers)
				fmt.Println("AvgBatchSize : ", response.AvgBatchSize)
				fmt.Println("TableSize : ", response.TableSize)
				fmt.Println("NumRows : ", response.NumRows)
			}
		}
	}
	return err
}

package stash

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/lensesio/tableprinter"
	"github.com/urfave/cli/v2"

	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/client"
)

type DestinationCountResult struct {
	Count    int    `header:"Count"`
	DestName string `header:"Destination"`
	Error    string `header:"Error"`
}

func Query(c *cli.Context) error {
	var reply string
	err := client.GetUDSClient().Call("ProcErrors.GetDSStats", c.String("dsnum"), &reply)
	if err == nil {
		if c.String("format") == "json" {
			fmt.Println(reply)
		} else {
			var response []DestinationCountResult
			printer := tableprinter.New(os.Stdout)
			printer.BorderTop, printer.BorderBottom, printer.BorderLeft, printer.BorderRight = true, true, true, true
			printer.CenterSeparator = "│"
			printer.ColumnSeparator = "│"
			printer.RowSeparator = "─"
			err = json.Unmarshal([]byte(reply), &response)
			if err == nil {
				fmt.Println("ProcErrorsByDestinationCount")
				fmt.Println("================================================================================")
				printer.Print(response)
			}
		}
	}
	return err
}

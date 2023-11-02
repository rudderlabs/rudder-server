package warehouse

import (
	"fmt"
	"os"

	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli/v2"

	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/client"
)

type QueryResult struct {
	Columns []string
	Values  [][]string
}

type QueryInput struct {
	DestID       string
	SourceID     string
	SQLStatement string
}

type ConfigurationTestInput struct {
	DestID string
}

type ConfigurationTestOutput struct {
	Valid bool
	Error string
}

func Query(c *cli.Context) (err error) {
	reply := QueryResult{}

	sqlStmt := c.String("sql")
	if c.IsSet("file") {
		var content []byte
		content, err = os.ReadFile(c.String("file"))
		if err != nil {
			return
		}
		sqlStmt = string(content)
	}

	input := QueryInput{
		DestID:       c.String("dest"),
		SourceID:     c.String("source"),
		SQLStatement: sqlStmt,
	}
	err = client.GetUDSClient().Call("Warehouse.Query", input, &reply)
	if err != nil {
		return
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(reply.Columns)
	table.SetAutoFormatHeaders(false)
	var headerColors []tablewriter.Colors
	for i := 0; i < len(reply.Columns); i++ {
		headerColors = append(headerColors, tablewriter.Colors{tablewriter.Bold, tablewriter.BgCyanColor})
	}
	table.SetHeaderColor(headerColors...)

	for _, v := range reply.Values {
		table.Append(v)
	}
	table.Render()
	return
}

func ConfigurationTest(c *cli.Context) (err error) {
	reply := ConfigurationTestOutput{}

	input := ConfigurationTestInput{
		DestID: c.String("dest"),
	}

	err = client.GetUDSClient().Call("Warehouse.ConfigurationTest", input, &reply)
	if err != nil {
		return
	}

	if reply.Valid {
		fmt.Printf("Successfully validated destID: %s \n", input.DestID)
	} else {
		fmt.Printf("Failed validation for destID: %s with err: %s \n", input.DestID, reply.Error)
	}
	return
}

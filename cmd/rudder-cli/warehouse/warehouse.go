package warehouse

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli/v2"

	"github.com/rudderlabs/rudder-go-kit/config"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/client"
	"github.com/rudderlabs/rudder-server/services/controlplane"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
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

func ConfigurationTest(c *cli.Context) error {
	misc.Init()
	backendconfig.Init()
	warehouseutils.Init()
	validations.Init()

	revisionID := c.String("revisionID")

	if strings.TrimSpace(revisionID) == "" {
		return errors.New("please specify the revision ID")
	}

	if err := backendconfig.Setup(nil); err != nil {
		return fmt.Errorf("setting up backend config: %w", err)
	}
	cpClient := controlplane.NewClient(
		config.GetString("CONFIG_BACKEND_URL", "https://api.rudderstack.com"),
		backendconfig.DefaultBackendConfig.Identity(),
	)
	destination, err := cpClient.DestinationHistory(c.Context, revisionID)
	if err != nil {
		return fmt.Errorf("getting destination history failed: %v", err)
	}
	fmt.Println("Destination history fetched successfully")

	res := validations.NewDestinationValidator().Validate(c.Context, &destination)
	if res.Success {
		fmt.Println(fmt.Printf("Successfully validated destination with revision ID: %s", revisionID))
	} else {
		fmt.Println(fmt.Printf("Failed to validate destination with revision ID: %s with error: %s", revisionID, res.Error))
	}
	return nil
}

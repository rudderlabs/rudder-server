package warehouse

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/samber/lo"
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
	revisionIDs := c.StringSlice("revisionIDs")
	revisionFile := c.String("file")

	fmt.Println("revisionID: ", revisionID)
	fmt.Println("revisionIDs: ", revisionIDs)
	fmt.Println("revisionFile: ", revisionFile)

	if strings.TrimSpace(revisionID) == "" && len(revisionIDs) == 0 && strings.TrimSpace(revisionFile) == "" {
		return fmt.Errorf("revisionID or revisionIDs is required")
	}
	if err := backendconfig.Setup(nil); err != nil {
		return fmt.Errorf("setting up backend config: %w", err)
	}

	cpClient := controlplane.NewClient(
		config.GetString("CONFIG_BACKEND_URL", "https://api.rudderstack.com"),
		backendconfig.DefaultBackendConfig.Identity(),
	)

	var revisionIDsToValidate []string

	if len(revisionFile) > 0 {
		data, err := os.ReadFile(revisionFile)
		if err != nil {
			return fmt.Errorf("reading revision file failed with error: %w", err)
		}

		revisionIDsToValidate = strings.Split(string(data), ",")
	} else if revisionID != "" {
		revisionIDsToValidate = append(revisionIDsToValidate, revisionID)
	} else {
		revisionIDsToValidate = revisionIDs
	}
	if len(revisionIDsToValidate) == 0 {
		return fmt.Errorf("revisions are required")
	}

	for i := range revisionIDsToValidate {
		revisionIDsToValidate[i] = strings.TrimSpace(revisionIDsToValidate[i])
	}

	output := map[string]string{}

	for i, revisionID := range revisionIDsToValidate {
		fmt.Println(fmt.Sprintf("Validating destination %d of %d with revision ID: %s", i+1, len(revisionIDsToValidate), revisionID))

		destination, err := cpClient.DestinationHistory(c.Context, revisionID)
		if err != nil {
			return fmt.Errorf("getting destination history failed for revision ID: %s with error: %w", revisionID, err)
		}
		fmt.Println("Destination history fetched successfully")

		res := validations.NewDestinationValidator().Validate(c.Context, &destination)
		if res.Success {
			output[revisionID] = fmt.Sprintf("Successfully validated destination with revision ID: %s", revisionID)
		} else {
			output[revisionID] = fmt.Sprintf("Failed to validate destination with revision ID: %s with error: %s", revisionID, res.Error)
		}
		fmt.Println(output[revisionID])

		time.Sleep(1 * time.Second)
	}

	fmt.Println("Failed destinations revision IDs:")
	fmt.Println(lo.Keys(output))

	columns := []string{"Revision ID", "Status"}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(columns)
	table.SetAutoFormatHeaders(false)
	table.SetHeaderColor(lo.Map(columns, func(item string, index int) tablewriter.Colors {
		return tablewriter.Colors{tablewriter.Bold, tablewriter.BgCyanColor}
	})...)
	for revisionID, status := range output {
		table.Append([]string{revisionID, status})
	}
	table.Render()
	return nil
}

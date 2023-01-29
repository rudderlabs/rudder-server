package schemarepository

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/glue"
	"regexp"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/logger"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

var (
	exclusionColumnsRegex = regexp.MustCompile(`(^.*_com$)|(^.*_in$)|(^.*_[0-9]{10,10}$)`)
)

func TestCaratlane(t *testing.T) {
	misc.Init()
	warehouseutils.Init()

	type S3Credentials struct {
		AccessKeyID string
		AccessKey   string
		Region      string
	}

	var (
		credentials = S3Credentials{
			Region:      "***",
			AccessKeyID: "***",
			AccessKey:   "***",
		}
		namespace = "***"
	)

	destination := backendconfig.DestinationT{
		Config: map[string]interface{}{
			"region":      credentials.Region,
			"accessKeyID": credentials.AccessKeyID,
			"accessKey":   credentials.AccessKey,
			"useGlue":     true,
		},
	}
	warehouse := warehouseutils.Warehouse{
		Destination: destination,
		Namespace:   namespace,
	}

	// Create a new schema repository
	g, err := NewGlueSchemaRepository(warehouse)
	g.Logger = logger.NOP
	require.NoError(t, err)

	// Fetching the schema from the warehouse
	schema, unrecognizedSchema, err := g.FetchSchema(warehouse)
	require.NoError(t, err)
	require.NotEmpty(t, schema)
	require.Empty(t, unrecognizedSchema)

	for table, _ := range schema {
		t.Logf("Table: %s contains %d columns", table, len(schema[table]))

		tableSchema := schema[table]
		require.NotEmpty(t, tableSchema)

		// Deleting the columns which matches the exclusion regex
		for column, _ := range tableSchema {
			if exclusionColumnsRegex.MatchString(column) {
				t.Logf("Skipping column: %s", column)
				delete(tableSchema, column)
			}
		}

		// Updating the schema in the warehouse
		_, err = g.GlueClient.UpdateTable(&glue.UpdateTableInput{
			DatabaseName: aws.String(namespace),
			TableInput: &glue.TableInput{
				Name:              aws.String(table),
				StorageDescriptor: g.getStorageDescriptor(table, tableSchema),
			},
		})
		require.NoError(t, err)
	}
}

func TestExclusionColumnsRegex(t *testing.T) {
	testCases := []struct {
		columnName string
		expected   bool
	}{
		{columnName: "test_com", expected: true},
		{columnName: "test_in", expected: true},
		{columnName: "test_1234567890", expected: true},
		{columnName: "test_com_1234567890", expected: true},
		{columnName: "test_in_1234567890", expected: true},
		{columnName: "test_1234567890_com", expected: true},
		{columnName: "test_1234567890_in", expected: true},
		{columnName: "test_1234567890_1234567890", expected: true},

		{columnName: "test_com_", expected: false},
		{columnName: "test_in_", expected: false},
		{columnName: "test_1234567890_", expected: false},
		{columnName: "test_123456789", expected: false},
		{columnName: "test", expected: false},
	}

	for _, tc := range testCases {
		require.Equal(t, tc.expected, exclusionColumnsRegex.MatchString(tc.columnName))
	}
}

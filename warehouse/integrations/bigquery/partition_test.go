package bigquery

import (
	"testing"
	"time"

	stdbigquery "cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-go-kit/config"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

func TestBigQuery_CheckValidPartitionColumn(t *testing.T) {
	testCases := []struct {
		name          string
		key           string
		expectedError error
	}{
		{
			name:          "valid partition column",
			key:           "loaded_at",
			expectedError: nil,
		},
		{
			name:          "invalid partition column",
			key:           "invalid",
			expectedError: errPartitionColumnNotSupported,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bq := &BigQuery{}
			bq.conf = config.New()
			bq.warehouse = model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"partitionColumn": tc.key,
					},
				},
			}
			err := bq.checkValidPartitionColumn(bq.partitionColumn())
			if tc.expectedError != nil {
				require.ErrorIs(t, err, tc.expectedError)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestBigQuery_BigqueryPartitionType(t *testing.T) {
	testCases := []struct {
		name                  string
		key                   string
		expectedError         error
		expectedPartitionType stdbigquery.TimePartitioningType
	}{
		{
			name:                  "valid partition type",
			key:                   "hour",
			expectedError:         nil,
			expectedPartitionType: stdbigquery.HourPartitioningType,
		},
		{
			name:          "invalid partition type",
			key:           "invalid",
			expectedError: errPartitionTypeNotSupported,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bq := &BigQuery{}
			bq.conf = config.New()
			bq.warehouse = model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"partitionType": tc.key,
					},
				},
			}
			output, err := bq.bigqueryPartitionType(bq.partitionType())
			if tc.expectedError != nil {
				require.ErrorIs(t, err, tc.expectedError)
				require.Empty(t, output)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedPartitionType, output)
		})
	}
}

func TestBigquery_PartitionDate(t *testing.T) {
	testCases := []struct {
		name              string
		config            map[string]interface{}
		expectedPartition string
		expectedError     error
	}{
		{
			name:              "ingestion partition",
			config:            map[string]interface{}{},
			expectedPartition: "2023-04-05",
		},
		{
			name: "hour partition type",
			config: map[string]interface{}{
				"partitionType": "hour",
			},
			expectedPartition: "2023-04-05T06",
			expectedError:     nil,
		},
		{
			name: "day partition type",
			config: map[string]interface{}{
				"partitionType": "day",
			},
			expectedPartition: "2023-04-05",
			expectedError:     nil,
		},
		{
			name: "unsupported partition type",
			config: map[string]interface{}{
				"partitionType": "invalid",
			},
			expectedPartition: "",
			expectedError:     errPartitionTypeNotSupported,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bq := &BigQuery{}
			bq.conf = config.New()
			bq.now = func() time.Time {
				return time.Date(2023, 4, 5, 6, 7, 8, 9, time.UTC)
			}
			bq.warehouse = model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: tc.config,
				},
			}
			output, err := bq.partitionDate()
			if tc.expectedError != nil {
				require.ErrorIs(t, err, tc.expectedError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedPartition, output)
		})
	}
}

func TestBigQuery_AvoidPartitionDecorator(t *testing.T) {
	testCases := []struct {
		name                    string
		destConfig              map[string]interface{}
		configOverride          map[string]interface{}
		avoidPartitionDecorator bool
	}{
		{
			name:                    "partition not defined",
			destConfig:              map[string]interface{}{},
			avoidPartitionDecorator: false,
		},
		{
			name: "loaded_at partition",
			destConfig: map[string]interface{}{
				"partitionColumn": "loaded_at",
			},
			avoidPartitionDecorator: true,
		},
		{
			name: "received_at partition",
			destConfig: map[string]interface{}{
				"partitionColumn": "received_at",
			},
			avoidPartitionDecorator: true,
		},
		{
			name: "sent_at partition",
			destConfig: map[string]interface{}{
				"partitionColumn": "sent_at",
			},
			avoidPartitionDecorator: true,
		},
		{
			name: "timestamp partition",
			destConfig: map[string]interface{}{
				"partitionColumn": "timestamp",
			},
			avoidPartitionDecorator: true,
		},
		{
			name: "original_timestamp partition",
			destConfig: map[string]interface{}{
				"partitionColumn": "received_at",
			},
			avoidPartitionDecorator: true,
		},
		{
			name: "ingestion partition",
			destConfig: map[string]interface{}{
				"partitionColumn": "_PARTITIONTIME",
			},
			avoidPartitionDecorator: false,
		},
		{
			name: "unsupported partition column",
			destConfig: map[string]interface{}{
				"partitionColumn": "invalid",
			},
			avoidPartitionDecorator: false,
		},
		{
			name:       "custom`partition enabled",
			destConfig: map[string]interface{}{},
			configOverride: map[string]interface{}{
				"Warehouse.bigquery.customPartitionsEnabled": true,
			},
			avoidPartitionDecorator: true,
		},
		{
			name:       "custom`partition enabled workspaceIDs",
			destConfig: map[string]interface{}{},
			configOverride: map[string]interface{}{
				"Warehouse.bigquery.customPartitionsEnabledWorkspaceIDs": "workspaceID",
			},
			avoidPartitionDecorator: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := config.New()
			for k, v := range tc.configOverride {
				c.Set(k, v)
			}

			bq := New(c, logger.NOP)
			bq.warehouse = model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: tc.destConfig,
				},
				WorkspaceID: "workspaceID",
			}
			require.Equal(t, tc.avoidPartitionDecorator, bq.avoidPartitionDecorator())
		})
	}
}

func TestPartitionedTable(t *testing.T) {
	testCases := []struct {
		name           string
		partitionTable string
		expected       string
	}{
		{
			name:           "partitioned table",
			partitionTable: "2023-04-05T06",
			expected:       "table$2023040506",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, partitionedTable("table", tc.partitionTable))
		})
	}
}

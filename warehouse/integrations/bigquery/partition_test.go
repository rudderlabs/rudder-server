package bigquery

import (
	"testing"
	"time"

	stdbigquery "cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"

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
			name: "month partition type",
			config: map[string]interface{}{
				"partitionType": "month",
			},
			expectedPartition: "2023-04",
			expectedError:     nil,
		},
		{
			name: "year partition type",
			config: map[string]interface{}{
				"partitionType": "year",
			},
			expectedPartition: "2023",
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

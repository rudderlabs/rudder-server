package salesforcebulk

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

func TestSalesforceBulk_parseDestinationConfig(t *testing.T) {
	testCases := []struct {
		name        string
		destination *backendconfig.DestinationT
		expected    DestinationConfig
		wantErr     bool
		errorMsg    string
	}{
		{
			name: "valid config with all fields",
			destination: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"rudderAccountId": "test-account-123",
					"operation":       "insert",
					"apiVersion":      "v57.0",
				},
			},
			expected: DestinationConfig{
				RudderAccountID: "test-account-123",
				Operation:       "insert",
				APIVersion:      "v57.0",
			},
			wantErr: false,
		},
		{
			name: "valid config with defaults",
			destination: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"rudderAccountId": "test-account-456",
				},
			},
			expected: DestinationConfig{
				RudderAccountID: "test-account-456",
				Operation:       "",
				APIVersion:      "",
			},
			wantErr: false,
		},
		{
			name: "missing rudderAccountId",
			destination: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"operation": "insert",
				},
			},
			wantErr:  true,
			errorMsg: "rudderAccountId is required",
		},
		{
			name: "invalid operation",
			destination: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"rudderAccountId": "test-account-789",
					"operation":       "invalid_op",
				},
			},
			wantErr:  true,
			errorMsg: "invalid operation",
		},
		{
			name: "upsert operation",
			destination: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"rudderAccountId": "test-account-upsert",
					"operation":       "upsert",
				},
			},
			expected: DestinationConfig{
				RudderAccountID: "test-account-upsert",
				Operation:       "upsert",
			},
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result, err := parseDestinationConfig(tc.destination)

			if tc.wantErr {
				require.Error(t, err)
				if tc.errorMsg != "" {
					require.Contains(t, err.Error(), tc.errorMsg)
				}
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expected.RudderAccountID, result.RudderAccountID)
			require.Equal(t, tc.expected.Operation, result.Operation)
			if tc.expected.APIVersion != "" {
				require.Equal(t, tc.expected.APIVersion, result.APIVersion)
			}
		})
	}
}

func TestSalesforceBulk_extractObjectInfo(t *testing.T) {
	testCases := []struct {
		name     string
		jobs     []common.AsyncJob
		expected *ObjectInfo
		wantErr  bool
		errorMsg string
	}{
		{
			name: "valid externalId with Contact object",
			jobs: []common.AsyncJob{
				{
					Message: map[string]interface{}{
						"Email": "test@example.com",
					},
					Metadata: map[string]interface{}{
						"job_id": float64(1),
						"externalId": []interface{}{
							map[string]interface{}{
								"type":           "Salesforce-Contact",
								"id":             "test@example.com",
								"identifierType": "Email",
							},
						},
					},
				},
			},
			expected: &ObjectInfo{
				ObjectType:      "Contact",
				ExternalIDField: "Email",
			},
			wantErr: false,
		},
		{
			name: "valid externalId with Lead object",
			jobs: []common.AsyncJob{
				{
					Message: map[string]interface{}{
						"Email": "lead@example.com",
					},
					Metadata: map[string]interface{}{
						"job_id": float64(2),
						"externalId": []interface{}{
							map[string]interface{}{
								"type":           "Salesforce-Lead",
								"id":             "lead@example.com",
								"identifierType": "Email",
							},
						},
					},
				},
			},
			expected: &ObjectInfo{
				ObjectType:      "Lead",
				ExternalIDField: "Email",
			},
			wantErr: false,
		},
		{
			name:     "empty jobs array",
			jobs:     []common.AsyncJob{},
			wantErr:  true,
			errorMsg: "no jobs to process",
		},
		{
			name: "missing externalId - falls back to config",
			jobs: []common.AsyncJob{
				{
					Message: map[string]interface{}{
						"Email": "test@example.com",
					},
					Metadata: map[string]interface{}{
						"job_id": float64(3),
					},
				},
			},
			expected: &ObjectInfo{
				ObjectType:      "Lead",
				ExternalIDField: "Email",
			},
			wantErr: false,
		},
		{
			name: "empty externalId array",
			jobs: []common.AsyncJob{
				{
					Message: map[string]interface{}{},
					Metadata: map[string]interface{}{
						"externalId": []interface{}{},
					},
				},
			},
			wantErr:  true,
			errorMsg: "at least one element",
		},
		{
			name: "event stream without externalId - uses config",
			jobs: []common.AsyncJob{
				{
					Message: map[string]interface{}{
						"Email": "stream@example.com",
					},
					Metadata: map[string]interface{}{
						"job_id": float64(10),
					},
				},
			},
			expected: &ObjectInfo{
				ObjectType:      "Contact",
				ExternalIDField: "Email",
			},
			wantErr: false,
		},
		{
			name: "event stream with default object type",
			jobs: []common.AsyncJob{
				{
					Message: map[string]interface{}{
						"Email": "default@example.com",
					},
					Metadata: map[string]interface{}{
						"job_id": float64(11),
					},
				},
			},
			expected: &ObjectInfo{
				ObjectType:      "Lead",
				ExternalIDField: "Email",
			},
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			testConfig := DestinationConfig{}
			if tc.name == "event stream without externalId - uses config" {
				testConfig.ObjectType = "Contact"
			}

			result, err := extractObjectInfo(tc.jobs, testConfig)

			if tc.wantErr {
				require.Error(t, err)
				if tc.errorMsg != "" {
					require.Contains(t, err.Error(), tc.errorMsg)
				}
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expected.ObjectType, result.ObjectType)
			require.Equal(t, tc.expected.ExternalIDField, result.ExternalIDField)
		})
	}
}

func TestSalesforceBulk_createCSVFile(t *testing.T) {
	testCases := []struct {
		name              string
		jobs              []common.AsyncJob
		expectedInserted  int
		expectedOverflow  int
		wantErr           bool
	}{
		{
			name: "create CSV with valid jobs",
			jobs: []common.AsyncJob{
				{
					Message: map[string]interface{}{
						"Email":     "test1@example.com",
						"FirstName": "John",
						"LastName":  "Doe",
					},
					Metadata: map[string]interface{}{
						"job_id": float64(1),
					},
				},
				{
					Message: map[string]interface{}{
						"Email":     "test2@example.com",
						"FirstName": "Jane",
						"LastName":  "Smith",
					},
					Metadata: map[string]interface{}{
						"job_id": float64(2),
					},
				},
			},
			expectedInserted: 2,
			expectedOverflow: 0,
			wantErr:          false,
		},
		{
			name:    "empty jobs array",
			jobs:    []common.AsyncJob{},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dataHashToJobID := make(map[string]int64)
			csvFilePath, headers, insertedJobIDs, overflowedJobIDs, err := createCSVFile(
				"test-dest-123",
				tc.jobs,
				dataHashToJobID,
			)

			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotEmpty(t, csvFilePath)
			require.NotEmpty(t, headers)
			require.Len(t, insertedJobIDs, tc.expectedInserted)
			require.Len(t, overflowedJobIDs, tc.expectedOverflow)

			_, err = os.Stat(csvFilePath)
			require.NoError(t, err)

			t.Cleanup(func() {
				require.NoError(t, os.Remove(csvFilePath))
			})

			require.Len(t, dataHashToJobID, tc.expectedInserted)
		})
	}
}

func TestSalesforceBulk_calculateHashCode(t *testing.T) {
	testCases := []struct {
		name     string
		row      []string
		expected string
	}{
		{
			name:     "simple row",
			row:      []string{"test@example.com", "John", "Doe"},
			expected: calculateHashCode([]string{"test@example.com", "John", "Doe"}),
		},
		{
			name:     "empty row",
			row:      []string{},
			expected: calculateHashCode([]string{}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := calculateHashCode(tc.row)

			require.NotEmpty(t, result)
			require.Equal(t, tc.expected, result)

			result2 := calculateHashCode(tc.row)
			require.Equal(t, result, result2)
		})
	}
}

func TestSalesforceBulk_calculateHashFromRecord(t *testing.T) {
	testCases := []struct {
		name        string
		record      map[string]string
		csvHeaders  []string
		expected    string
		shouldMatch bool
		compareWith []string
	}{
		{
			name: "match original CSV row - ignores Salesforce columns",
			record: map[string]string{
				"Email":       "test@example.com",
				"FirstName":   "John",
				"LastName":    "Doe",
				"sf__Id":      "003xx000004TmiQAAS",
				"sf__Created": "true",
				"sf__Error":   "",
			},
			csvHeaders:  []string{"Email", "FirstName", "LastName"},
			shouldMatch: true,
			compareWith: []string{"test@example.com", "John", "Doe"},
		},
		{
			name: "handles user's sf__ fields correctly",
			record: map[string]string{
				"Email":             "user@example.com",
				"sf__AccountStatus": "Active",
				"FirstName":         "Jane",
				"sf__Id":            "003xx000005TmiQBBT",
			},
			csvHeaders:  []string{"Email", "FirstName", "sf__AccountStatus"},
			shouldMatch: true,
			compareWith: []string{"user@example.com", "Jane", "Active"},
		},
		{
			name: "consistent hash with sorted headers",
			record: map[string]string{
				"LastName":  "Smith",
				"Email":     "smith@example.com",
				"FirstName": "Bob",
			},
			csvHeaders:  []string{"Email", "FirstName", "LastName"},
			shouldMatch: true,
			compareWith: []string{"smith@example.com", "Bob", "Smith"},
		},
		{
			name: "handles missing fields with empty strings",
			record: map[string]string{
				"Email":     "partial@example.com",
				"FirstName": "Only",
			},
			csvHeaders:  []string{"Email", "FirstName", "LastName"},
			shouldMatch: true,
			compareWith: []string{"partial@example.com", "Only", ""},
		},
		{
			name:        "empty record",
			record:      map[string]string{},
			csvHeaders:  []string{"Email", "FirstName"},
			shouldMatch: true,
			compareWith: []string{"", ""},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			hash := calculateHashFromRecord(tc.record, tc.csvHeaders)

			require.NotEmpty(t, hash)

			if tc.shouldMatch {
				expectedHash := calculateHashCode(tc.compareWith)
				require.Equal(t, expectedHash, hash,
					"Hash from record should match hash from original CSV values")
			}

			hash2 := calculateHashFromRecord(tc.record, tc.csvHeaders)
			require.Equal(t, hash, hash2, "Hash should be consistent across multiple calls")
		})
	}
}

func TestSalesforceBulk_calculateHashFromRecord_Integration(t *testing.T) {
	t.Run("upload and result matching flow", func(t *testing.T) {
		t.Parallel()

		csvHeaders := []string{"Email", "FirstName", "LastName"}
		uploadRow := []string{"integration@example.com", "Test", "User"}
		uploadHash := calculateHashCode(uploadRow)

		salesforceResult := map[string]string{
			"Email":       "integration@example.com",
			"FirstName":   "Test",
			"LastName":    "User",
			"sf__Id":      "003xx000006TmiQCCU",
			"sf__Created": "true",
			"sf__Error":   "",
		}
		resultHash := calculateHashFromRecord(salesforceResult, csvHeaders)

		require.Equal(t, uploadHash, resultHash,
			"Upload hash and result hash should match for same data")
	})

	t.Run("different data produces different hashes", func(t *testing.T) {
		t.Parallel()

		csvHeaders := []string{"Email", "FirstName"}

		record1 := map[string]string{
			"Email":     "user1@example.com",
			"FirstName": "User",
		}
		record2 := map[string]string{
			"Email":     "user2@example.com",
			"FirstName": "User",
		}

		hash1 := calculateHashFromRecord(record1, csvHeaders)
		hash2 := calculateHashFromRecord(record2, csvHeaders)

		require.NotEqual(t, hash1, hash2,
			"Different records should produce different hashes")
	})
}


package salesforcebulk

import (
	"encoding/csv"
	"os"
	"strings"
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

func TestSalesforceBulk_extractObjectInfoFromJob(t *testing.T) {
	testCases := []struct {
		name     string
		job      common.AsyncJob
		config   DestinationConfig
		expected *ObjectInfo
		wantErr  bool
		errorMsg string
	}{
		{
			name: "valid externalId with Contact object",
			job: common.AsyncJob{
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
			expected: &ObjectInfo{
				ObjectType:      "Contact",
				ExternalIDField: "Email",
			},
		},
		{
			name: "valid externalId with Lead object",
			job: common.AsyncJob{
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
			expected: &ObjectInfo{
				ObjectType:      "Lead",
				ExternalIDField: "Email",
			},
		},
		{
			name: "missing externalId - falls back to config",
			job: common.AsyncJob{
				Message: map[string]interface{}{
					"Email": "test@example.com",
				},
				Metadata: map[string]interface{}{
					"job_id": float64(3),
				},
			},
			config: DestinationConfig{ObjectType: "Contact"},
			expected: &ObjectInfo{
				ObjectType:      "Contact",
				ExternalIDField: "Email",
			},
		},
		{
			name: "missing externalId - defaults to Lead when config empty",
			job: common.AsyncJob{
				Message: map[string]interface{}{
					"Email": "default@example.com",
				},
				Metadata: map[string]interface{}{
					"job_id": float64(4),
				},
			},
			expected: &ObjectInfo{
				ObjectType:      "Lead",
				ExternalIDField: "Email",
			},
		},
		{
			name: "empty externalId array",
			job: common.AsyncJob{
				Message: map[string]interface{}{},
				Metadata: map[string]interface{}{
					"externalId": []interface{}{},
				},
			},
			wantErr:  true,
			errorMsg: "at least one element",
		},
		{
			name: "metadata matches identifier injected into message",
			job: common.AsyncJob{
				Message: map[string]interface{}{
					"External_Id__c": "ACC123",
				},
				Metadata: map[string]interface{}{
					"job_id": float64(12),
					"externalId": []interface{}{
						map[string]interface{}{
							"type": "Salesforce-Lead",
						},
						map[string]interface{}{
							"type":           "Salesforce-Account",
							"id":             "ACC123",
							"identifierType": "External_Id__c",
						},
					},
				},
			},
			expected: &ObjectInfo{
				ObjectType:      "Account",
				ExternalIDField: "External_Id__c",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result, err := extractObjectInfoFromJob(tc.job, tc.config)

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

func TestSalesforceBulk_extractFromVDM_DefaultIdentifierType(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name             string
		externalIDRaw    interface{}
		expectedObject   string
		expectedField    string
		expectError      bool
		expectedErrorMsg string
	}{
		{
			name: "identifierType defaults to Id when empty",
			externalIDRaw: []interface{}{
				map[string]interface{}{
					"type":           "Salesforce-Account",
					"id":             "001XYZ",
					"identifierType": "",
				},
			},
			expectedObject: "Account",
			expectedField:  "Id",
		},
		{
			name: "identifierType defaults to Id when missing",
			externalIDRaw: []interface{}{
				map[string]interface{}{
					"type": "Salesforce-Contact",
					"id":   "003XYZ",
				},
			},
			expectedObject: "Contact",
			expectedField:  "Id",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			objectInfo, err := extractFromVDM(tc.externalIDRaw, map[string]interface{}{})
			if tc.expectError {
				require.Error(t, err)
				if tc.expectedErrorMsg != "" {
					require.Contains(t, err.Error(), tc.expectedErrorMsg)
				}
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expectedObject, objectInfo.ObjectType)
			require.Equal(t, tc.expectedField, objectInfo.ExternalIDField)
		})
	}
}

func TestSalesforceBulk_extractFromVDM_SelectsMatchingEntry(t *testing.T) {
	t.Parallel()

	metadata := []interface{}{
		map[string]interface{}{
			"type": "Salesforce-Lead",
		},
		map[string]interface{}{
			"type":           "Salesforce-Account",
			"id":             "ACC123",
			"identifierType": "External_Id__c",
		},
	}

	message := map[string]interface{}{
		"External_Id__c": "ACC123",
	}

	objectInfo, err := extractFromVDM(metadata, message)
	require.NoError(t, err)
	require.Equal(t, "Account", objectInfo.ObjectType)
	require.Equal(t, "External_Id__c", objectInfo.ExternalIDField)
}

func TestSalesforceBulk_createCSVFile(t *testing.T) {
	testCases := []struct {
		name             string
		jobs             []common.AsyncJob
		expectedInserted int
		expectedOverflow int
		wantErr          bool
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

			dataHashToJobID := make(map[string][]int64)
			csvFilePath, headers, insertedJobIDs, overflowedJobs, err := createCSVFile(
				"test-dest-123",
				tc.jobs,
				dataHashToJobID,
				"upsert",
			)

			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotEmpty(t, csvFilePath)
			require.NotEmpty(t, headers)
			require.Len(t, insertedJobIDs, tc.expectedInserted)
			require.Len(t, overflowedJobs, tc.expectedOverflow)

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

			hash := calculateHashFromRecord(tc.record, tc.csvHeaders, "delete")

			require.NotEmpty(t, hash)

			if tc.shouldMatch {
				expectedHash := calculateHashWithOperation(tc.compareWith, "delete")
				require.Equal(t, expectedHash, hash,
					"Hash from record should match hash from original CSV values")
			}

			hash2 := calculateHashFromRecord(tc.record, tc.csvHeaders, "delete")
			require.Equal(t, hash, hash2, "Hash should be consistent across multiple calls")
		})
	}
}

func TestSalesforceBulk_calculateHashFromRecord_Integration(t *testing.T) {
	t.Run("upload and result matching flow", func(t *testing.T) {
		t.Parallel()

		csvHeaders := []string{"Email", "FirstName", "LastName"}
		uploadRow := []string{"integration@example.com", "Test", "User"}
		uploadHash := calculateHashWithOperation(uploadRow, "update")

		salesforceResult := map[string]string{
			"Email":       "integration@example.com",
			"FirstName":   "Test",
			"LastName":    "User",
			"sf__Id":      "003xx000006TmiQCCU",
			"sf__Created": "true",
			"sf__Error":   "",
		}
		resultHash := calculateHashFromRecord(salesforceResult, csvHeaders, "update")

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

		hash1 := calculateHashFromRecord(record1, csvHeaders, "upsert")
		hash2 := calculateHashFromRecord(record2, csvHeaders, "upsert")

		require.NotEqual(t, hash1, hash2,
			"Different records should produce different hashes")
	})
}

func TestSalesforceBulk_createCSVFile_VaryingFields(t *testing.T) {
	t.Run("jobs with different fields get union of all fields in CSV", func(t *testing.T) {
		jobs := []common.AsyncJob{
			{
				Message: map[string]interface{}{
					"Email":     "user1@example.com",
					"FirstName": "John",
				},
				Metadata: map[string]interface{}{"job_id": float64(1)},
			},
			{
				Message: map[string]interface{}{
					"Email":    "user2@example.com",
					"LastName": "Smith",
					"Phone":    "555-1234",
				},
				Metadata: map[string]interface{}{"job_id": float64(2)},
			},
			{
				Message: map[string]interface{}{
					"Email":     "user3@example.com",
					"FirstName": "Jane",
					"LastName":  "Doe",
					"Company":   "Acme Inc",
				},
				Metadata: map[string]interface{}{"job_id": float64(3)},
			},
		}

		dataHashToJobID := make(map[string][]int64)
		csvFilePath, headers, insertedJobIDs, _, err := createCSVFile(
			"test-dest",
			jobs,
			dataHashToJobID,
			"update",
		)

		require.NoError(t, err)
		defer os.Remove(csvFilePath)

		require.Len(t, insertedJobIDs, 3)
		require.Contains(t, headers, "Email")
		require.Contains(t, headers, "FirstName")
		require.Contains(t, headers, "LastName")
		require.Contains(t, headers, "Phone")
		require.Contains(t, headers, "Company")

		file, err := os.Open(csvFilePath)
		require.NoError(t, err)
		defer file.Close()

		reader := csv.NewReader(file)
		records, err := reader.ReadAll()
		require.NoError(t, err)
		require.Len(t, records, 4)

		headerRow := records[0]
		require.Len(t, headerRow, 5)

		for i, row := range records[1:] {
			require.Len(t, row, 5, "Row %d should have 5 columns", i+1)
		}
	})
}

func TestSalesforceBulk_createCSVFile_SingleRowTooLarge(t *testing.T) {
	t.Run("returns empty insertedJobIDs when single row exceeds limit", func(t *testing.T) {
		hugeData := strings.Repeat("x", 101*1024*1024)

		jobs := []common.AsyncJob{
			{
				Message: map[string]interface{}{
					"Email": "test@example.com",
					"Data":  hugeData,
				},
				Metadata: map[string]interface{}{"job_id": float64(1)},
			},
		}

		dataHashToJobID := make(map[string][]int64)
		csvFilePath, headers, insertedJobIDs, overflowedJobs, err := createCSVFile(
			"test-dest",
			jobs,
			dataHashToJobID,
			"insert",
		)

		require.NoError(t, err)
		if csvFilePath != "" {
			defer os.Remove(csvFilePath)
		}

		require.Empty(t, insertedJobIDs, "No jobs should fit when row exceeds 100MB")
		require.Len(t, overflowedJobs, 1, "Job should be in overflow")
		require.Equal(t, int64(1), int64(overflowedJobs[0].Metadata["job_id"].(float64)))
		require.NotEmpty(t, headers, "Headers should still be created")
	})
}

func TestSalesforceBulk_extractOperationFromJob(t *testing.T) {
	testCases := []struct {
		name              string
		job               common.AsyncJob
		defaultOperation  string
		expectedOperation string
	}{
		{
			name: "extract operation from rudderOperation field",
			job: common.AsyncJob{
				Message: map[string]interface{}{
					"Email":           "test@example.com",
					"rudderOperation": "delete",
				},
			},
			defaultOperation:  "insert",
			expectedOperation: "delete",
		},
		{
			name: "fallback to default when no rudderOperation",
			job: common.AsyncJob{
				Message: map[string]interface{}{
					"Email": "test@example.com",
				},
			},
			defaultOperation:  "update",
			expectedOperation: "update",
		},
		{
			name: "fallback to default when rudderOperation is empty",
			job: common.AsyncJob{
				Message: map[string]interface{}{
					"Email":           "test@example.com",
					"rudderOperation": "",
				},
			},
			defaultOperation:  "insert",
			expectedOperation: "insert",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := extractOperationFromJob(tc.job, tc.defaultOperation)

			require.Equal(t, tc.expectedOperation, result)
		})
	}
}

func TestSalesforceBulk_groupJobsByOperation(t *testing.T) {
	testCases := []struct {
		name             string
		jobs             []common.AsyncJob
		defaultOperation string
		expectedGroups   map[string]int
	}{
		{
			name: "mixed operations",
			jobs: []common.AsyncJob{
				{
					Message: map[string]interface{}{
						"Email":           "insert@example.com",
						"rudderOperation": "insert",
					},
				},
				{
					Message: map[string]interface{}{
						"Email":           "update@example.com",
						"rudderOperation": "update",
					},
				},
				{
					Message: map[string]interface{}{
						"Email":           "delete@example.com",
						"rudderOperation": "delete",
					},
				},
				{
					Message: map[string]interface{}{
						"Email":           "insert2@example.com",
						"rudderOperation": "insert",
					},
				},
			},
			defaultOperation: "insert",
			expectedGroups: map[string]int{
				"insert": 2,
				"update": 1,
				"delete": 1,
			},
		},
		{
			name: "all default operation",
			jobs: []common.AsyncJob{
				{
					Message: map[string]interface{}{
						"Email": "test1@example.com",
					},
				},
				{
					Message: map[string]interface{}{
						"Email": "test2@example.com",
					},
				},
			},
			defaultOperation: "upsert",
			expectedGroups: map[string]int{
				"upsert": 2,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := groupJobsByOperation(tc.jobs, tc.defaultOperation)

			require.Len(t, result, len(tc.expectedGroups))
			for operation, expectedCount := range tc.expectedGroups {
				require.Len(t, result[operation], expectedCount,
					"Operation %s should have %d jobs", operation, expectedCount)
			}
		})
	}
}

package salesforcebulkupload

import (
	"encoding/csv"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

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
								"type":           "SALESFORCE_BULK_UPLOAD-Contact",
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
								"type":           "SALESFORCE_BULK_UPLOAD-Lead",
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
			wantErr:  true,
			errorMsg: "externalId not found in the first job",
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
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result, err := extractObjectInfo(tc.jobs)

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
						"externalId": []interface{}{
							map[string]interface{}{
								"type":           "SALESFORCE_BULK_UPLOAD-Contact",
								"id":             "test1@example.com",
								"identifierType": "Email",
							},
						},
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
						"externalId": []interface{}{
							map[string]interface{}{
								"type":           "SALESFORCE_BULK_UPLOAD-Contact",
								"id":             "test2@example.com",
								"identifierType": "Email",
							},
						},
					},
				},
			},
			expectedInserted: 2,
			expectedOverflow: 0,
			wantErr:          false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			csvFilePath, headers, hashToJobID, err := createCSVFile(
				"test-dest-123",
				tc.jobs,
			)

			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotEmpty(t, csvFilePath)
			require.NotEmpty(t, headers)

			_, err = os.Stat(csvFilePath)
			require.NoError(t, err)

			t.Cleanup(func() {
				require.NoError(t, os.Remove(csvFilePath))
			})

			require.Len(t, hashToJobID, tc.expectedInserted)
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

		csvFilePath, headers, hashToJobID, err := createCSVFile(
			"test-dest",
			jobs,
		)

		require.NoError(t, err)
		defer os.Remove(csvFilePath)

		require.Len(t, hashToJobID, 3)
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

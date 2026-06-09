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
					Message: map[string]any{
						"Email": "test@example.com",
					},
					Metadata: map[string]any{
						"job_id": float64(1),
						"externalId": []any{
							map[string]any{
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
					Message: map[string]any{
						"Email": "lead@example.com",
					},
					Metadata: map[string]any{
						"job_id": float64(2),
						"externalId": []any{
							map[string]any{
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
					Message: map[string]any{
						"Email": "test@example.com",
					},
					Metadata: map[string]any{
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
					Message: map[string]any{},
					Metadata: map[string]any{
						"externalId": []any{},
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
					Message: map[string]any{
						"Email":     "test1@example.com",
						"FirstName": "John",
						"LastName":  "Doe",
					},
					Metadata: map[string]any{
						"job_id": float64(1),
						"externalId": []any{
							map[string]any{
								"type":           "SALESFORCE_BULK_UPLOAD-Contact",
								"id":             "test1@example.com",
								"identifierType": "Email",
							},
						},
					},
				},
				{
					Message: map[string]any{
						"Email":     "test2@example.com",
						"FirstName": "Jane",
						"LastName":  "Smith",
					},
					Metadata: map[string]any{
						"job_id": float64(2),
						"externalId": []any{
							map[string]any{
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
				"Email",
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
	t.Run("upload key and result key match for the same externalId, regardless of other columns", func(t *testing.T) {
		t.Parallel()

		// What we sent: the row keyed only on the externalId (Email) value.
		uploadKey := calculateHashCode("integration@example.com")

		// What Salesforce returns: the externalId round-trips unchanged even
		// though the CreatedDate column was truncated from microseconds to
		// milliseconds on store.
		resultRecord := map[string]string{
			"Email":       "integration@example.com",
			"CreatedDate": "2025-11-25T03:45:21.142Z", // sent .14287Z, stored .142Z
			"sf__Id":      "003xx000006TmiQCCU",
			"sf__Created": "true",
		}
		resultKey := calculateHashCode(resultRecord["Email"])

		require.Equal(t, uploadKey, resultKey,
			"externalId-based key must survive Salesforce coercion of other columns")
	})

	t.Run("different externalIds produce different keys", func(t *testing.T) {
		t.Parallel()
		require.NotEqual(t,
			calculateHashCode("user1@example.com"),
			calculateHashCode("user2@example.com"),
		)
	})

	t.Run("consistent across calls and never empty", func(t *testing.T) {
		t.Parallel()
		require.NotEmpty(t, calculateHashCode("a@b.com"))
		require.Equal(t, calculateHashCode("a@b.com"), calculateHashCode("a@b.com"))
	})
}

func TestSalesforceBulk_createCSVFile_NumericNoScientificNotation(t *testing.T) {
	t.Parallel()
	jobs := []common.AsyncJob{
		{
			Message: map[string]any{
				"Email":          "user@example.com",
				"Account_Number": float64(1234567890),
				"Account_IDs":    []any{float64(1234567890), float64(9876543210)},
			},
			Metadata: map[string]any{"job_id": float64(1)},
		},
	}

	csvFilePath, _, _, err := createCSVFile("test-dest-numeric", "Email", jobs)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(csvFilePath) })

	file, err := os.Open(csvFilePath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })

	records, err := csv.NewReader(file).ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 2)
	require.Equal(t, []string{"Account_IDs", "Account_Number", "Email"}, records[0])
	require.Equal(t, []string{"[1234567890,9876543210]", "1234567890", "user@example.com"}, records[1])
}

func TestSalesforceBulk_createCSVFile_NullValues(t *testing.T) {
	jobs := []common.AsyncJob{
		{
			Message: map[string]any{
				"Email":     "middle@example.com",
				"FirstName": "Mid",
				"LastName":  nil,
				"Phone":     "555-0001",
			},
			Metadata: map[string]any{"job_id": float64(1)},
		},
		{
			Message: map[string]any{
				"Email":     "trailing@example.com",
				"FirstName": "End",
				"LastName":  "User",
				"Phone":     nil,
			},
			Metadata: map[string]any{"job_id": float64(2)},
		},
	}

	csvFilePath, _, _, err := createCSVFile("test-dest-null", "Email", jobs)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(csvFilePath) })

	file, err := os.Open(csvFilePath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })

	records, err := csv.NewReader(file).ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 3)
	require.Equal(t, []string{"Email", "FirstName", "LastName", "Phone"}, records[0])
	require.Equal(t, []string{"middle@example.com", "Mid", "", "555-0001"}, records[1])
	require.Equal(t, []string{"trailing@example.com", "End", "User", ""}, records[2])
}

func TestSalesforceBulk_createCSVFile_VaryingFields(t *testing.T) {
	t.Run("jobs with different fields get union of all fields in CSV", func(t *testing.T) {
		jobs := []common.AsyncJob{
			{
				Message: map[string]any{
					"Email":     "user1@example.com",
					"FirstName": "John",
				},
				Metadata: map[string]any{"job_id": float64(1)},
			},
			{
				Message: map[string]any{
					"Email":    "user2@example.com",
					"LastName": "Smith",
					"Phone":    "555-1234",
				},
				Metadata: map[string]any{"job_id": float64(2)},
			},
			{
				Message: map[string]any{
					"Email":     "user3@example.com",
					"FirstName": "Jane",
					"LastName":  "Doe",
					"Company":   "Acme Inc",
				},
				Metadata: map[string]any{"job_id": float64(3)},
			},
		}

		csvFilePath, headers, hashToJobID, err := createCSVFile(
			"test-dest",
			"Email",
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

package salesforcebulkupload

import (
	"encoding/csv"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSalesforceBulk_extractObjectInfo(t *testing.T) {
	testCases := []struct {
		name     string
		jobs     []SalesforceAsyncJob
		expected *ObjectInfo
		wantErr  bool
		errorMsg string
	}{
		{
			name: "valid externalId with Contact object",
			jobs: []SalesforceAsyncJob{
				{
					Message: map[string]any{"Email": "test@example.com"},
					Metadata: SalesforceJobMetadata{
						JobID: 1,
						ExternalID: []SalesforceExternalID{
							{Type: "SALESFORCE_BULK_UPLOAD-Contact", ID: "test@example.com", IdentifierType: "Email"},
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
			jobs: []SalesforceAsyncJob{
				{
					Message: map[string]any{"Email": "lead@example.com"},
					Metadata: SalesforceJobMetadata{
						JobID: 2,
						ExternalID: []SalesforceExternalID{
							{Type: "SALESFORCE_BULK_UPLOAD-Lead", ID: "lead@example.com", IdentifierType: "Email"},
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
			jobs:     []SalesforceAsyncJob{},
			wantErr:  true,
			errorMsg: "no jobs to process",
		},
		{
			name: "missing externalId",
			jobs: []SalesforceAsyncJob{
				{
					Message:  map[string]any{"Email": "test@example.com"},
					Metadata: SalesforceJobMetadata{JobID: 3},
				},
			},
			wantErr:  true,
			errorMsg: "externalId not found in the first job",
		},
		{
			name: "empty externalId array",
			jobs: []SalesforceAsyncJob{
				{
					Message:  map[string]any{},
					Metadata: SalesforceJobMetadata{ExternalID: []SalesforceExternalID{}},
				},
			},
			wantErr:  true,
			errorMsg: "externalId not found in the first job",
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
		jobs             []SalesforceAsyncJob
		expectedInserted int
		wantErr          bool
	}{
		{
			name: "create CSV with valid jobs",
			jobs: []SalesforceAsyncJob{
				{
					Message:  map[string]any{"Email": "test1@example.com", "FirstName": "John", "LastName": "Doe"},
					Metadata: SalesforceJobMetadata{JobID: 1},
				},
				{
					Message:  map[string]any{"Email": "test2@example.com", "FirstName": "Jane", "LastName": "Smith"},
					Metadata: SalesforceJobMetadata{JobID: 2},
				},
			},
			expectedInserted: 2,
			wantErr:          false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			csvFilePath, externalIDToJobID, fileSize, err := createCSVFile(
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

			info, err := os.Stat(csvFilePath)
			require.NoError(t, err)
			require.Equal(t, info.Size(), fileSize, "returned size should match the file on disk")

			t.Cleanup(func() {
				require.NoError(t, os.Remove(csvFilePath))
			})

			require.Len(t, externalIDToJobID, tc.expectedInserted)
		})
	}
}

func TestSalesforceBulk_createCSVFile_NumericNoScientificNotation(t *testing.T) {
	t.Parallel()
	jobs := []SalesforceAsyncJob{
		{
			Message: map[string]any{
				"Email":          "user@example.com",
				"Account_Number": float64(1234567890),
				"Account_IDs":    []any{float64(1234567890), float64(9876543210)},
			},
			Metadata: SalesforceJobMetadata{JobID: 1},
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
	jobs := []SalesforceAsyncJob{
		{
			Message: map[string]any{
				"Email":     "middle@example.com",
				"FirstName": "Mid",
				"LastName":  nil,
				"Phone":     "555-0001",
			},
			Metadata: SalesforceJobMetadata{JobID: 1},
		},
		{
			Message: map[string]any{
				"Email":     "trailing@example.com",
				"FirstName": "End",
				"LastName":  "User",
				"Phone":     nil,
			},
			Metadata: SalesforceJobMetadata{JobID: 2},
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
		jobs := []SalesforceAsyncJob{
			{
				Message:  map[string]any{"Email": "user1@example.com", "FirstName": "John"},
				Metadata: SalesforceJobMetadata{JobID: 1},
			},
			{
				Message:  map[string]any{"Email": "user2@example.com", "LastName": "Smith", "Phone": "555-1234"},
				Metadata: SalesforceJobMetadata{JobID: 2},
			},
			{
				Message:  map[string]any{"Email": "user3@example.com", "FirstName": "Jane", "LastName": "Doe", "Company": "Acme Inc"},
				Metadata: SalesforceJobMetadata{JobID: 3},
			},
		}

		csvFilePath, externalIDToJobID, _, err := createCSVFile(
			"test-dest",
			"Email",
			jobs,
		)

		require.NoError(t, err)
		defer os.Remove(csvFilePath)

		require.Len(t, externalIDToJobID, 3)

		file, err := os.Open(csvFilePath)
		require.NoError(t, err)
		defer file.Close()

		reader := csv.NewReader(file)
		records, err := reader.ReadAll()
		require.NoError(t, err)
		require.Len(t, records, 4)

		headerRow := records[0]
		require.Len(t, headerRow, 5)
		require.Contains(t, headerRow, "Email")
		require.Contains(t, headerRow, "FirstName")
		require.Contains(t, headerRow, "LastName")
		require.Contains(t, headerRow, "Phone")
		require.Contains(t, headerRow, "Company")

		for i, row := range records[1:] {
			require.Len(t, row, 5, "Row %d should have 5 columns", i+1)
		}
	})
}

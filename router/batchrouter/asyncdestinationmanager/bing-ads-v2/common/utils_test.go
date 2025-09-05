package common

import (
	"archive/zip"
	"encoding/csv"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetAbortedReasons(t *testing.T) {
	tests := []struct {
		name           string
		clientIDErrors map[int64]map[string]struct{}
		want           map[int64][]string // compare as slice of errors
	}{
		{
			name:           "empty input",
			clientIDErrors: map[int64]map[string]struct{}{},
			want:           map[int64][]string{},
		},
		{
			name: "single client single error",
			clientIDErrors: map[int64]map[string]struct{}{
				1: {"ErrorA": {}},
			},
			want: map[int64][]string{
				1: {"ErrorA"},
			},
		},
		{
			name: "single client multiple errors",
			clientIDErrors: map[int64]map[string]struct{}{
				2: {"ErrorA": {}, "ErrorB": {}},
			},
			want: map[int64][]string{
				2: {"ErrorA", "ErrorB"},
			},
		},
		{
			name: "multiple clients",
			clientIDErrors: map[int64]map[string]struct{}{
				1: {"ErrorA": {}},
				2: {"ErrorB": {}, "ErrorC": {}},
			},
			want: map[int64][]string{
				1: {"ErrorA"},
				2: {"ErrorB", "ErrorC"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetAbortedReasons(tt.clientIDErrors)

			// check each client ID
			require.Equal(t, len(tt.want), len(got))
			for id, expectedErrors := range tt.want {
				gotStr, ok := got[id]
				require.True(t, ok)

				// split back into slice
				gotErrors := strings.Split(gotStr, CommaSeparator)

				// compare as sets (order independent)
				require.ElementsMatch(t, expectedErrors, gotErrors)
			}
		})
	}
}

func TestGetSuccessJobIDs(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name             string
		failedEventList  []int64
		initialEventList []int64
		expected         []int64
	}{
		{
			name:             "all jobs successful",
			failedEventList:  []int64{},
			initialEventList: []int64{1, 2, 3, 4, 5},
			expected:         []int64{1, 2, 3, 4, 5},
		},
		{
			name:             "some jobs failed",
			failedEventList:  []int64{2, 4},
			initialEventList: []int64{1, 2, 3, 4, 5},
			expected:         []int64{1, 3, 5},
		},
		{
			name:             "all jobs failed",
			failedEventList:  []int64{1, 2, 3, 4, 5},
			initialEventList: []int64{1, 2, 3, 4, 5},
			expected:         []int64{},
		},
		{
			name:             "empty initial list",
			failedEventList:  []int64{1, 2, 3},
			initialEventList: []int64{},
			expected:         []int64{},
		},
		{
			name:             "empty failed list",
			failedEventList:  []int64{},
			initialEventList: []int64{1, 2, 3},
			expected:         []int64{1, 2, 3},
		},
		{
			name:             "duplicate job IDs",
			failedEventList:  []int64{2, 2, 4},
			initialEventList: []int64{1, 2, 3, 4, 5},
			expected:         []int64{1, 3, 5},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := GetSuccessJobIDs(tc.failedEventList, tc.initialEventList)
			require.ElementsMatch(t, tc.expected, result)
		})
	}
}

func TestConvertCSVToZip(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		eventCount  int64
		setupFiles  func(t *testing.T) *ActionFileInfo
		wantErr     bool
		expectedErr string
	}{
		{
			name:       "successful conversion",
			eventCount: 5,
			setupFiles: func(t *testing.T) *ActionFileInfo {
				tempDir := t.TempDir()
				csvPath := filepath.Join(tempDir, "test.csv")
				zipPath := filepath.Join(tempDir, "test.zip")

				// Create a test CSV file
				csvFile, err := os.Create(csvPath)
				require.NoError(t, err)
				writer := csv.NewWriter(csvFile)
				require.NoError(t, writer.Write([]string{"header1", "header2"}))
				require.NoError(t, writer.Write([]string{"value1", "value2"}))
				writer.Flush()
				csvFile.Close()

				return &ActionFileInfo{
					CSVFilePath: csvPath,
					ZipFilePath: zipPath,
					EventCount:  5,
				}
			},
			wantErr: false,
		},
		{
			name:       "zero event count - removes files",
			eventCount: 0,
			setupFiles: func(t *testing.T) *ActionFileInfo {
				tempDir := t.TempDir()
				csvPath := filepath.Join(tempDir, "test.csv")
				zipPath := filepath.Join(tempDir, "test.zip")

				// Create empty files
				require.NoError(t, os.WriteFile(csvPath, []byte(""), 0o644))
				require.NoError(t, os.WriteFile(zipPath, []byte(""), 0o644))

				return &ActionFileInfo{
					CSVFilePath: csvPath,
					ZipFilePath: zipPath,
					EventCount:  0,
				}
			},
			wantErr: false,
		},
		{
			name:       "invalid zip file path",
			eventCount: 5,
			setupFiles: func(t *testing.T) *ActionFileInfo {
				tempDir := t.TempDir()
				csvPath := filepath.Join(tempDir, "test.csv")
				zipPath := "/invalid/path/test.zip" // Invalid path

				// Create a test CSV file
				csvFile, err := os.Create(csvPath)
				require.NoError(t, err)
				writer := csv.NewWriter(csvFile)
				require.NoError(t, writer.Write([]string{"header1", "header2"}))
				writer.Flush()
				csvFile.Close()

				return &ActionFileInfo{
					CSVFilePath: csvPath,
					ZipFilePath: zipPath,
					EventCount:  5,
				}
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			actionFile := tc.setupFiles(t)

			err := ConvertCSVToZip(actionFile)

			if tc.wantErr {
				require.Error(t, err)
				if tc.expectedErr != "" {
					require.Contains(t, err.Error(), tc.expectedErr)
				}
			} else {
				require.NoError(t, err)

				if tc.eventCount == 0 {
					// Check that files were removed
					_, err := os.Stat(actionFile.CSVFilePath)
					require.True(t, os.IsNotExist(err))

					_, err = os.Stat(actionFile.ZipFilePath)
					require.True(t, os.IsNotExist(err))
				} else {
					// Check that zip file was created and contains the CSV
					_, err := os.Stat(actionFile.ZipFilePath)
					require.NoError(t, err)

					// Verify zip contents
					reader, err := zip.OpenReader(actionFile.ZipFilePath)
					require.NoError(t, err)
					defer reader.Close()

					found := false
					for _, file := range reader.File {
						if file.Name == filepath.Base(actionFile.CSVFilePath) {
							found = true
							break
						}
					}
					require.True(t, found, "CSV file not found in zip")

					// Check that CSV file was removed
					_, err = os.Stat(actionFile.CSVFilePath)
					require.True(t, os.IsNotExist(err))
				}
			}
		})
	}
}

func TestUnzip(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		setupZip    func(t *testing.T) (string, string) // returns zipPath, targetDir
		wantErr     bool
		expectedErr string
		checkFiles  func(t *testing.T, filePaths []string, targetDir string)
	}{
		{
			name: "successful unzip with files",
			setupZip: func(t *testing.T) (string, string) {
				tempDir := t.TempDir()
				zipPath := filepath.Join(tempDir, "test.zip")
				targetDir := filepath.Join(tempDir, "output")

				// Create a test zip file
				zipFile, err := os.Create(zipPath)
				require.NoError(t, err)

				zipWriter := zip.NewWriter(zipFile)

				// Add a test file to zip
				testFile, err := zipWriter.Create("test.txt")
				require.NoError(t, err)
				_, err = testFile.Write([]byte("test content"))
				require.NoError(t, err)

				// Close the zip writer before closing the file
				zipWriter.Close()
				zipFile.Close()

				return zipPath, targetDir
			},
			wantErr: false,
			checkFiles: func(t *testing.T, filePaths []string, targetDir string) {
				require.Len(t, filePaths, 1)
				require.Equal(t, filepath.Join(targetDir, "test.txt"), filePaths[0])

				// Check file content
				content, err := os.ReadFile(filePaths[0])
				require.NoError(t, err)
				require.Equal(t, "test content", string(content))
			},
		},
		{
			name: "successful unzip with nested files",
			setupZip: func(t *testing.T) (string, string) {
				tempDir := t.TempDir()
				zipPath := filepath.Join(tempDir, "test.zip")
				targetDir := filepath.Join(tempDir, "output")

				// Create a test zip file with nested file structure
				zipFile, err := os.Create(zipPath)
				require.NoError(t, err)

				zipWriter := zip.NewWriter(zipFile)

				// Add a file in subdirectory (this will create the directory automatically)
				testFile, err := zipWriter.Create("subdir/test.txt")
				require.NoError(t, err)
				_, err = testFile.Write([]byte("test content"))
				require.NoError(t, err)

				// Close the zip writer before closing the file
				zipWriter.Close()
				zipFile.Close()

				return zipPath, targetDir
			},
			wantErr: false,
			checkFiles: func(t *testing.T, filePaths []string, targetDir string) {
				require.Len(t, filePaths, 1)
				require.Equal(t, filepath.Join(targetDir, "subdir/test.txt"), filePaths[0])

				// Check directory was created (but not included in filePaths)
				subdirPath := filepath.Join(targetDir, "subdir")
				info, err := os.Stat(subdirPath)
				require.NoError(t, err)
				require.True(t, info.IsDir())
			},
		},
		{
			name: "invalid zip file",
			setupZip: func(t *testing.T) (string, string) {
				tempDir := t.TempDir()
				zipPath := filepath.Join(tempDir, "invalid.zip")
				targetDir := filepath.Join(tempDir, "output")

				// Create an invalid zip file
				require.NoError(t, os.WriteFile(zipPath, []byte("not a zip file"), 0o644))

				return zipPath, targetDir
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			zipPath, targetDir := tc.setupZip(t)

			filePaths, err := Unzip(zipPath, targetDir)

			if tc.wantErr {
				require.Error(t, err)
				if tc.expectedErr != "" {
					require.Contains(t, err.Error(), tc.expectedErr)
				}
			} else {
				require.NoError(t, err)
				if tc.checkFiles != nil {
					tc.checkFiles(t, filePaths, targetDir)
				}
			}
		})
	}
}

func TestDownloadAndGetUploadStatusFile(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name            string
		destinationName string
		setupServer     func() (*httptest.Server, string)
		wantErr         bool
		expectedErr     string
	}{
		{
			name:            "successful download and extract",
			destinationName: "test-dest",
			setupServer: func() (*httptest.Server, string) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					// Create a simple zip file in memory
					zipFile, err := os.CreateTemp("", "test_*.zip")
					require.NoError(t, err)
					defer os.Remove(zipFile.Name())

					zipWriter := zip.NewWriter(zipFile)
					testFile, err := zipWriter.Create("test.txt")
					require.NoError(t, err)
					_, err = testFile.Write([]byte("test content"))
					require.NoError(t, err)
					zipWriter.Close()

					// Read the zip file and serve it
					zipData, err := os.ReadFile(zipFile.Name())
					require.NoError(t, err)

					w.Header().Set("Content-Type", "application/zip")
					_, err = w.Write(zipData)
					require.NoError(t, err)
				}))

				return server, server.URL
			},
			wantErr: false,
		},
		{
			name:            "URL with &amp; entities",
			destinationName: "test-dest",
			setupServer: func() (*httptest.Server, string) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					// Check that &amp; was converted to &
					require.Contains(t, r.URL.RawQuery, "&")
					require.NotContains(t, r.URL.RawQuery, "&amp;")

					// Create a simple zip file
					zipFile, err := os.CreateTemp("", "test_*.zip")
					require.NoError(t, err)
					defer os.Remove(zipFile.Name())

					zipWriter := zip.NewWriter(zipFile)
					testFile, err := zipWriter.Create("test.txt")
					require.NoError(t, err)
					_, err = testFile.Write([]byte("test content"))
					require.NoError(t, err)
					zipWriter.Close()

					zipData, err := os.ReadFile(zipFile.Name())
					require.NoError(t, err)

					w.Header().Set("Content-Type", "application/zip")
					_, err = w.Write(zipData)
					require.NoError(t, err)
				}))

				return server, server.URL + "?param1=value1&amp;param2=value2"
			},
			wantErr: false,
		},
		{
			name:            "download failure network error",
			destinationName: "test-dest",
			setupServer: func() (*httptest.Server, string) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					conn, _, _ := w.(http.Hijacker).Hijack()
					conn.Close()
				}))
				return server, server.URL
			},
			wantErr:     true,
			expectedErr: "failed downloading zip file",
		},
		{
			name:            "download success but invalid zip",
			destinationName: "test-dest",
			setupServer: func() (*httptest.Server, string) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					// Serve invalid zip content
					w.Header().Set("Content-Type", "application/zip")
					_, err := w.Write([]byte("not a valid zip file"))
					require.NoError(t, err)
				}))
				return server, server.URL
			},
			wantErr:     true,
			expectedErr: "failed extracting zip file",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var server *httptest.Server
			var testURL string
			if tc.setupServer != nil {
				server, testURL = tc.setupServer()
				if server != nil {
					defer server.Close()
				}
			}

			filePaths, err := DownloadAndGetUploadStatusFile(tc.destinationName, testURL)

			if tc.wantErr {
				require.Error(t, err)
				if tc.expectedErr != "" {
					require.Contains(t, err.Error(), tc.expectedErr)
				}
			} else {
				require.NoError(t, err)
				require.NotEmpty(t, filePaths)

				// Check that files were extracted
				for _, filePath := range filePaths {
					_, err := os.Stat(filePath)
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestReadAndCleanupCSV(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		setupCSV     func(t *testing.T) string
		wantErr      bool
		expectedErr  string
		checkRecords func(t *testing.T, records [][]string)
	}{
		{
			name: "successful read and cleanup",
			setupCSV: func(t *testing.T) string {
				tempDir := t.TempDir()
				csvPath := filepath.Join(tempDir, "test.csv")

				// Create a test CSV file
				csvFile, err := os.Create(csvPath)
				require.NoError(t, err)
				writer := csv.NewWriter(csvFile)
				require.NoError(t, writer.Write([]string{"header1", "header2"}))
				require.NoError(t, writer.Write([]string{"value1", "value2"}))
				require.NoError(t, writer.Write([]string{"value3", "value4"}))
				writer.Flush()
				csvFile.Close()

				return csvPath
			},
			wantErr: false,
			checkRecords: func(t *testing.T, records [][]string) {
				require.Len(t, records, 3)
				require.Equal(t, []string{"header1", "header2"}, records[0])
				require.Equal(t, []string{"value1", "value2"}, records[1])
				require.Equal(t, []string{"value3", "value4"}, records[2])
			},
		},
		{
			name: "empty CSV file",
			setupCSV: func(t *testing.T) string {
				tempDir := t.TempDir()
				csvPath := filepath.Join(tempDir, "test.csv")

				// Create an empty CSV file
				require.NoError(t, os.WriteFile(csvPath, []byte(""), 0o644))

				return csvPath
			},
			wantErr: false,
			checkRecords: func(t *testing.T, records [][]string) {
				require.Len(t, records, 0)
			},
		},
		{
			name: "file not found",
			setupCSV: func(t *testing.T) string {
				tempDir := t.TempDir()
				return filepath.Join(tempDir, "nonexistent.csv")
			},
			wantErr: true,
		},
		{
			name: "invalid CSV format",
			setupCSV: func(t *testing.T) string {
				tempDir := t.TempDir()
				csvPath := filepath.Join(tempDir, "test.csv")

				// Create a file with invalid CSV content
				require.NoError(t, os.WriteFile(csvPath, []byte("invalid,csv,content\nwith\nmultiple\nlines"), 0o644))

				return csvPath
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			csvPath := tc.setupCSV(t)

			records, err := ReadAndCleanupCSV(csvPath)

			if tc.wantErr {
				require.Error(t, err)
				if tc.expectedErr != "" {
					require.Contains(t, err.Error(), tc.expectedErr)
				}
			} else {
				require.NoError(t, err)
				if tc.checkRecords != nil {
					tc.checkRecords(t, records)
				}
			}

			// Check that file was cleaned up
			_, err = os.Stat(csvPath)
			require.True(t, os.IsNotExist(err))
		})
	}
}

func TestCreateActionFile(t *testing.T) {
	t.Run("successful file creation", func(t *testing.T) {
		templateFunc := func(csvFile *os.File) (*csv.Writer, error) {
			writer := csv.NewWriter(csvFile)
			err := writer.Write([]string{"header1", "header2"})
			return writer, err
		}

		actionFile, err := CreateActionFile("test", templateFunc)
		require.NoError(t, err)
		require.NotNil(t, actionFile)
		require.NotEmpty(t, actionFile.CSVFilePath)
		require.NotEmpty(t, actionFile.ZipFilePath)
		require.NotNil(t, actionFile.CSVWriter)
		require.Equal(t, "test", actionFile.Action)

		// Clean up - only remove CSV file since zip file doesn't exist yet
		t.Cleanup(func() {
			require.NoError(t, os.Remove(actionFile.CSVFilePath))
			// Don't try to remove zip file as it's not created until needed
		})
	})

	t.Run("template function error", func(t *testing.T) {
		templateFunc := func(csvFile *os.File) (*csv.Writer, error) {
			return nil, fmt.Errorf("template error")
		}

		actionFile, err := CreateActionFile("test", templateFunc)
		require.Error(t, err)
		require.Nil(t, actionFile)
		require.Contains(t, err.Error(), "template error")
	})
}

func TestProcessPollStatusData(t *testing.T) {
	testCases := []struct {
		name           string
		records        [][]string
		config         PollStatusConfig
		expectedResult map[int64]map[string]struct{}
		expectedError  string
	}{
		{
			name: "successful processing with audience format",
			records: [][]string{
				{"Type", "Client Id", "Error"},
				{"Customer List Item Error", "1<<>>client1", "error1"},
				{"Customer List Item Error", "1<<>>client2", "error2"},
				{"Customer List Item Error", "2<<>>client3", "error1"},
				{"Success", "3<<>>client4", "no error"},
			},
			config: PollStatusConfig{
				IDColumnName:    "Client Id",
				ErrorColumnName: "Error",
				TypeColumnIndex: 0,
				ErrorTypeFilter: "Customer List Item Error",
				ParseIDFunc:     ParseClientID,
			},
			expectedResult: map[int64]map[string]struct{}{
				1: {"error1": {}, "error2": {}},
				2: {"error1": {}},
			},
		},
		{
			name: "successful processing with offline-conversions format",
			records: [][]string{
				{"Type", "Id", "Error"},
				{"Error", "1", "error1"},
				{"Error", "1", "error2"},
				{"Error", "2", "error1"},
				{"Success", "3", "no error"},
			},
			config: PollStatusConfig{
				IDColumnName:    "Id",
				ErrorColumnName: "Error",
				TypeColumnIndex: 0,
				ErrorTypeFilter: "Error",
				ParseIDFunc:     ParseJobID,
			},
			expectedResult: map[int64]map[string]struct{}{
				1: {"error1": {}, "error2": {}},
				2: {"error1": {}},
			},
		},
		{
			name:    "empty records",
			records: [][]string{},
			config: PollStatusConfig{
				IDColumnName:    "Id",
				ErrorColumnName: "Error",
				TypeColumnIndex: 0,
				ErrorTypeFilter: "Error",
				ParseIDFunc:     ParseJobID,
			},
			expectedResult: map[int64]map[string]struct{}{},
		},
		{
			name: "only header row",
			records: [][]string{
				{"Type", "Id", "Error"},
			},
			config: PollStatusConfig{
				IDColumnName:    "Id",
				ErrorColumnName: "Error",
				TypeColumnIndex: 0,
				ErrorTypeFilter: "Error",
				ParseIDFunc:     ParseJobID,
			},
			expectedResult: map[int64]map[string]struct{}{},
		},
		{
			name: "missing ID column",
			records: [][]string{
				{"Type", "Error"},
				{"Error", "error1"},
			},
			config: PollStatusConfig{
				IDColumnName:    "Id",
				ErrorColumnName: "Error",
				TypeColumnIndex: 0,
				ErrorTypeFilter: "Error",
				ParseIDFunc:     ParseJobID,
			},
			expectedError: "required column 'Id' not found in header",
		},
		{
			name: "missing Error column",
			records: [][]string{
				{"Type", "Id"},
				{"Error", "1"},
			},
			config: PollStatusConfig{
				IDColumnName:    "Id",
				ErrorColumnName: "Error",
				TypeColumnIndex: 0,
				ErrorTypeFilter: "Error",
				ParseIDFunc:     ParseJobID,
			},
			expectedError: "required column 'Error' not found in header",
		},
		{
			name: "parse error in audience format",
			records: [][]string{
				{"Type", "Client Id", "Error"},
				{"Customer List Item Error", "invalid_format", "error1"},
			},
			config: PollStatusConfig{
				IDColumnName:    "Client Id",
				ErrorColumnName: "Error",
				TypeColumnIndex: 0,
				ErrorTypeFilter: "Customer List Item Error",
				ParseIDFunc:     ParseClientID,
			},
			expectedError: "invalid client id format: invalid_format",
		},
		{
			name: "parse error in offline-conversions format",
			records: [][]string{
				{"Type", "Id", "Error"},
				{"Error", "not_a_number", "error1"},
			},
			config: PollStatusConfig{
				IDColumnName:    "Id",
				ErrorColumnName: "Error",
				TypeColumnIndex: 0,
				ErrorTypeFilter: "Error",
				ParseIDFunc:     ParseJobID,
			},
			expectedError: "strconv.ParseInt: parsing \"not_a_number\": invalid syntax",
		},
		{
			name: "type column index out of bounds",
			records: [][]string{
				{"Type", "Id", "Error"},
				{"Error", "1", "error1"},
			},
			config: PollStatusConfig{
				IDColumnName:    "Id",
				ErrorColumnName: "Error",
				TypeColumnIndex: 5, // Out of bounds
				ErrorTypeFilter: "Error",
				ParseIDFunc:     ParseJobID,
			},
			expectedResult: map[int64]map[string]struct{}{},
		},
		{
			name: "duplicate errors for same ID",
			records: [][]string{
				{"Type", "Id", "Error"},
				{"Error", "1", "error1"},
				{"Error", "1", "error1"}, // Duplicate error
				{"Error", "1", "error2"},
			},
			config: PollStatusConfig{
				IDColumnName:    "Id",
				ErrorColumnName: "Error",
				TypeColumnIndex: 0,
				ErrorTypeFilter: "Error",
				ParseIDFunc:     ParseJobID,
			},
			expectedResult: map[int64]map[string]struct{}{
				1: {"error1": {}, "error2": {}}, // error1 appears only once due to map
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result, err := ProcessPollStatusData(tc.records, tc.config)

			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expectedResult, result)
		})
	}
}

func TestParseClientID(t *testing.T) {
	testCases := []struct {
		name          string
		clientID      string
		expectedJobID int64
		expectedError string
	}{
		{
			name:          "valid client ID format",
			clientID:      "123<<>>client@example.com",
			expectedJobID: 123,
		},
		{
			name:          "valid client ID with large job ID",
			clientID:      "999999999<<>>client@example.com",
			expectedJobID: 999999999,
		},
		{
			name:          "valid client ID with zero job ID",
			clientID:      "0<<>>client@example.com",
			expectedJobID: 0,
		},
		{
			name:          "valid client ID with negative job ID",
			clientID:      "-123<<>>client@example.com",
			expectedJobID: -123,
		},
		{
			name:          "missing separator",
			clientID:      "123client@example.com",
			expectedError: "invalid client id format: 123client@example.com",
		},
		{
			name:          "multiple separators",
			clientID:      "123<<>>client<<>>extra",
			expectedError: "invalid client id format: 123<<>>client<<>>extra",
		},
		{
			name:          "empty job ID part",
			clientID:      "<<>>client@example.com",
			expectedError: "invalid job id in clientId: <<>>client@example.com",
		},
		{
			name:          "empty client ID part",
			clientID:      "123<<>>",
			expectedError: "invalid job id in clientId: 123<<>>",
		},
		{
			name:          "non-numeric job ID",
			clientID:      "abc<<>>client@example.com",
			expectedError: "invalid job id in clientId: abc<<>>client@example.com",
		},
		{
			name:          "job ID with decimal",
			clientID:      "123.45<<>>client@example.com",
			expectedError: "invalid job id in clientId: 123.45<<>>client@example.com",
		},
		{
			name:          "empty string",
			clientID:      "",
			expectedError: "invalid client id format: ",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			jobID, err := ParseClientID(tc.clientID)

			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expectedJobID, jobID)
		})
	}
}

func TestParseJobID(t *testing.T) {
	testCases := []struct {
		name          string
		jobID         string
		expectedJobID int64
		expectedError string
	}{
		{
			name:          "valid positive job ID",
			jobID:         "123",
			expectedJobID: 123,
		},
		{
			name:          "valid zero job ID",
			jobID:         "0",
			expectedJobID: 0,
		},
		{
			name:          "valid negative job ID",
			jobID:         "-123",
			expectedJobID: -123,
		},
		{
			name:          "valid large job ID",
			jobID:         "999999999",
			expectedJobID: 999999999,
		},
		{
			name:          "non-numeric string",
			jobID:         "abc",
			expectedError: "strconv.ParseInt: parsing \"abc\": invalid syntax",
		},
		{
			name:          "decimal number",
			jobID:         "123.45",
			expectedError: "strconv.ParseInt: parsing \"123.45\": invalid syntax",
		},
		{
			name:          "empty string",
			jobID:         "",
			expectedError: "strconv.ParseInt: parsing \"\": invalid syntax",
		},
		{
			name:          "string with spaces",
			jobID:         " 123 ",
			expectedError: "strconv.ParseInt: parsing \" 123 \": invalid syntax",
		},
		{
			name:          "string with letters and numbers",
			jobID:         "123abc",
			expectedError: "strconv.ParseInt: parsing \"123abc\": invalid syntax",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			jobID, err := ParseJobID(tc.jobID)

			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expectedJobID, jobID)
		})
	}
}

func TestProcessActionFiles(t *testing.T) {
	t.Parallel()

	// Create mock action files with proper CSVWriter initialization
	actionFiles := map[string]*ActionFileInfo{
		"insert": {
			Action:           "insert",
			EventCount:       5,
			SuccessfulJobIDs: []int64{1, 2, 3, 4, 5},
			FailedJobIDs:     []int64{},
			CSVWriter:        csv.NewWriter(os.Stdout), // Mock CSV writer
		},
		"update": {
			Action:           "update",
			EventCount:       3,
			SuccessfulJobIDs: []int64{6, 7, 8},
			FailedJobIDs:     []int64{},
			CSVWriter:        csv.NewWriter(os.Stdout), // Mock CSV writer
		},
		"delete": {
			Action:           "delete",
			EventCount:       0,
			SuccessfulJobIDs: []int64{},
			FailedJobIDs:     []int64{},
			CSVWriter:        csv.NewWriter(os.Stdout), // Mock CSV writer
		},
	}

	actionTypes := []string{"insert", "update", "delete"}

	// Test successful processing
	result := ProcessActionFiles(actionFiles, actionTypes)

	// Should return only action files with events
	require.Len(t, result, 2)
	require.Equal(t, "insert", result[0].Action)
	require.Equal(t, "update", result[1].Action)
	require.Equal(t, int64(5), result[0].EventCount)
	require.Equal(t, int64(3), result[1].EventCount)
}

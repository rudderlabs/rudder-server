package marketobulkupload

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

// ==== Response Parsing Start ====

func TestCategorizeMarketoError(t *testing.T) {
	tests := []struct {
		name        string
		errorCode   string
		wantStatus  string
		wantMessage string
	}{
		{
			name:        "5XX error code",
			errorCode:   "502",
			wantStatus:  "Retryable",
			wantMessage: "Bad Gateway",
		},
		{
			name:        "Rate limit error",
			errorCode:   "606",
			wantStatus:  "Throttle",
			wantMessage: "Max rate limit exceeded",
		},
		{
			name:        "Invalid token",
			errorCode:   "601",
			wantStatus:  "RefreshToken",
			wantMessage: "Access token invalid",
		},
		{
			name:        "Invalid JSON",
			errorCode:   "609",
			wantStatus:  "Abortable",
			wantMessage: "Invalid JSON",
		},
		{
			name:        "Unknown error code",
			errorCode:   "999",
			wantStatus:  "Retryable",
			wantMessage: "Unknown",
		},
		{
			name:        "Invalid error code",
			errorCode:   "invalid",
			wantStatus:  "Retryable",
			wantMessage: "Unknown",
		},
		{
			name:        "Empty error code",
			errorCode:   "",
			wantStatus:  "Retryable",
			wantMessage: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStatus, gotMessage := categorizeMarketoError(tt.errorCode)
			if gotStatus != tt.wantStatus {
				t.Errorf("categorizeMarketoError() status = %v, want %v", gotStatus, tt.wantStatus)
			}
			if gotMessage != tt.wantMessage {
				t.Errorf("categorizeMarketoError() message = %v, want %v", gotMessage, tt.wantMessage)
			}
		})
	}
}

func TestHandleMarketoErrorCode(t *testing.T) {
	tests := []struct {
		name         string
		errorCode    string
		wantStatus   int
		wantCategory string
		wantMessage  string
	}{
		{
			name:         "Retryable error",
			errorCode:    "502",
			wantStatus:   500,
			wantCategory: "Retryable",
			wantMessage:  "Bad Gateway",
		},
		{
			name:         "Throttle error",
			errorCode:    "606",
			wantStatus:   429,
			wantCategory: "Throttle",
			wantMessage:  "Max rate limit exceeded",
		},
		{
			name:         "Abortable error",
			errorCode:    "609",
			wantStatus:   400,
			wantCategory: "Abortable",
			wantMessage:  "Invalid JSON",
		},
		{
			name:         "Refresh token error",
			errorCode:    "601",
			wantStatus:   500,
			wantCategory: "RefreshToken",
			wantMessage:  "Access token invalid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStatus, gotCategory, gotMessage := handleMarketoErrorCode(tt.errorCode)
			if gotStatus != tt.wantStatus {
				t.Errorf("handleMarketoErrorCode() status = %v, want %v", gotStatus, tt.wantStatus)
			}
			if gotCategory != tt.wantCategory {
				t.Errorf("handleMarketoErrorCode() category = %v, want %v", gotCategory, tt.wantCategory)
			}
			if gotMessage != tt.wantMessage {
				t.Errorf("handleMarketoErrorCode() message = %v, want %v", gotMessage, tt.wantMessage)
			}
		})
	}
}

func TestParseMarketoResponse(t *testing.T) {
	tests := []struct {
		name         string
		response     MarketoResponse
		wantStatus   int
		wantCategory string
		wantMessage  string
	}{
		{
			name: "Successful response",
			response: MarketoResponse{
				Success: true,
				Errors:  []Error{},
			},
			wantStatus:   200,
			wantCategory: "Success",
			wantMessage:  "",
		},
		{
			name: "Error response with code",
			response: MarketoResponse{
				Success: false,
				Errors: []Error{
					{
						Code:    "606",
						Message: "Rate limit exceeded",
					},
				},
			},
			wantStatus:   429,
			wantCategory: "Throttle",
			wantMessage:  "Rate limit exceeded: Max rate limit exceeded",
		},
		{
			name: "Error response without errors array",
			response: MarketoResponse{
				Success: false,
				Errors:  []Error{},
			},
			wantStatus:   500,
			wantCategory: "Retryable",
			wantMessage:  "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStatus, gotCategory, gotMessage := parseMarketoResponse(tt.response)
			if gotStatus != tt.wantStatus {
				t.Errorf("parseMarketoResponse() status = %v, want %v", gotStatus, tt.wantStatus)
			}
			if gotCategory != tt.wantCategory {
				t.Errorf("parseMarketoResponse() category = %v, want %v", gotCategory, tt.wantCategory)
			}
			if gotMessage != tt.wantMessage {
				t.Errorf("parseMarketoResponse() message = %v, want %v", gotMessage, tt.wantMessage)
			}
		})
	}
}

// ==== Response Parsing End ====

// ==== Hashing Test Start ====
func TestCalculateHashCode(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected string
	}{
		{
			name:     "Empty slice",
			input:    []string{},
			expected: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", // SHA256 hash of empty string
		},
		{
			name:     "Single value",
			input:    []string{"test"},
			expected: "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08", // SHA256 hash of "test"
		},
		{
			name:     "Multiple values",
			input:    []string{"value1", "value2", "value3"},
			expected: "ff5f3b7fdedbf50e0f312624b090b28cd61b50071cd10a775cb838deb078a6be", // SHA256 hash of "value1,value2,value3"
		},
		{
			name:     "Values with special characters",
			input:    []string{"hello,world", "test\nline", "special@char"},
			expected: "83905347e4ee7093c80675bcc6df55f6216105e2803a13c0ccb38edc88eee6aa", // actual hash will differ
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateHashCode(tt.input)
			if result != tt.expected {
				t.Errorf("calculateHashCode() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// ==== Hashing Test End ====

// ==== CSV File Creation Test Start ====

// Helper function to compare two slices for equality, ignoring the order
func slicesEqualIgnoreOrder(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	aMap := make(map[string]int)
	bMap := make(map[string]int)

	for _, item := range a {
		aMap[item]++
	}
	for _, item := range b {
		bMap[item]++
	}

	for key, count := range aMap {
		if bMap[key] != count {
			return false
		}
	}

	return true
}

func TestCreateCSVFile(t *testing.T) {
	// Create temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "marketo_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Set up test cases
	tests := []struct {
		name            string
		destinationID   string
		config          MarketoConfig
		input           []common.AsyncJob
		dataHashToJobId map[string]int64
		wantHeaders     []string
		wantInserted    []int64
		wantOverflowed  []int64
		wantError       bool
	}{
		{
			name:          "Basic CSV creation",
			destinationID: "test1",
			config: MarketoConfig{
				FieldsMapping: map[string]string{
					"email": "email",
					"name":  "fullName",
				},
			},
			input: []common.AsyncJob{
				{
					Message: map[string]interface{}{
						"email": "test@example.com",
						"name":  "Test User",
					},
					Metadata: map[string]interface{}{
						"job_id": float64(1),
					},
				},
			},
			dataHashToJobId: make(map[string]int64),
			wantHeaders:     []string{"email", "fullName"},
			wantInserted:    []int64{1},
			wantOverflowed:  []int64{},
			wantError:       false,
		},
		{
			name:          "File size overflow",
			destinationID: "test2",
			config: MarketoConfig{
				FieldsMapping: map[string]string{
					"email": "email",
					"name":  "fullName",
				},
			},
			input:           createLargeInputData(1000), // Helper function to create large dataset
			dataHashToJobId: make(map[string]int64),
			wantHeaders:     []string{"email", "fullName"},
			wantInserted:    nil, // Will be populated based on size calculations
			wantOverflowed:  nil, // Will be populated based on size calculations
			wantError:       false,
		},
		{
			name:          "Empty input",
			destinationID: "test3",
			config: MarketoConfig{
				FieldsMapping: map[string]string{
					"email": "email",
					"name":  "fullName",
				},
			},
			input:           []common.AsyncJob{},
			dataHashToJobId: make(map[string]int64),
			wantHeaders:     []string{"email", "fullName"},
			wantInserted:    []int64{},
			wantOverflowed:  []int64{},
			wantError:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Override the temporary directory for this test
			t.Setenv("TMPDIR", tmpDir)

			gotPath, gotHeaders, gotInserted, gotOverflowed, err := createCSVFile(
				tt.destinationID,
				tt.config,
				tt.input,
				tt.dataHashToJobId,
			)

			// Check error
			if (err != nil) != tt.wantError {
				t.Errorf("createCSVFile() error = %v, wantError %v", err, tt.wantError)
				return
			}

			// Verify the file exists and is readable
			if !tt.wantError {
				if _, err := os.Stat(gotPath); err != nil {
					t.Errorf("createCSVFile() created file does not exist: %v", err)
				}

				// Verify file contents
				file, err := os.Open(gotPath)
				if err != nil {
					t.Fatalf("Failed to open created file: %v", err)
				}
				defer file.Close()

				scanner := bufio.NewScanner(file)

				// Check header
				if scanner.Scan() {
					headerLine := scanner.Text()
					generatedHeaders := strings.Join(gotHeaders, ",")
					if headerLine != generatedHeaders {
						t.Errorf("createCSVFile() header from file and generated = %v, want %v", headerLine, generatedHeaders)
					}
				}

				// Verify returned headers match expected
				if !slicesEqualIgnoreOrder(gotHeaders, tt.wantHeaders) {
					t.Errorf("createCSVFile() headers = %v, want %v", gotHeaders, tt.wantHeaders)
				}

				// For the overflow test case, verify that some jobs were overflowed
				if tt.name == "File size overflow" {
					if len(gotOverflowed) == 0 {
						t.Error("createCSVFile() expected some overflowed jobs for large input")
					}
					if len(gotInserted)+len(gotOverflowed) != len(tt.input) {
						t.Error("createCSVFile() total of inserted and overflowed jobs doesn't match input length")
					}
				} else {
					// For other cases, verify exact matches
					if !reflect.DeepEqual(gotInserted, tt.wantInserted) {
						t.Errorf("createCSVFile() inserted = %v, want %v", gotInserted, tt.wantInserted)
					}
					if !reflect.DeepEqual(gotOverflowed, tt.wantOverflowed) {
						t.Errorf("createCSVFile() overflowed = %v, want %v", gotOverflowed, tt.wantOverflowed)
					}
				}
			}
		})
	}
}

// Helper function to create large input data for overflow testing
func createLargeInputData(count int) []common.AsyncJob {
	var result []common.AsyncJob
	for i := 0; i < count; i++ {
		job := common.AsyncJob{
			Message: map[string]interface{}{
				"email":    fmt.Sprintf("test%d@example.com", i),
				"fullName": strings.Repeat("Very long name ", 1000), // Create large content
			},
			Metadata: map[string]interface{}{
				"job_id": float64(i + 1),
			},
		}
		result = append(result, job)
	}
	return result
}

// ==== CSV File Creation Test End ====

// ==== CSV File Parsing Test Start ====

func TestReadJobsFromFile(t *testing.T) {
	// Create temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "jobs_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Helper function to create test file with content
	createTestFile := func(lines []string) string {
		file, err := os.CreateTemp(tmpDir, "test_jobs_*.txt")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		content := strings.Join(lines, "\n")
		if content != "" {
			content += "\n" // Add final newline
		}
		if _, err := file.WriteString(content); err != nil {
			t.Fatalf("Failed to write to temp file: %v", err)
		}
		file.Close()
		return file.Name()
	}

	// Sample jobs for testing
	job1 := common.AsyncJob{
		Message: map[string]interface{}{
			"email": "test1@example.com",
			"name":  "Test User 1",
		},
		Metadata: map[string]interface{}{
			"job_id": float64(1),
		},
	}
	job2 := common.AsyncJob{
		Message: map[string]interface{}{
			"email": "test2@example.com",
			"name":  "Test User 2",
		},
		Metadata: map[string]interface{}{
			"job_id": float64(2),
		},
	}

	// Convert jobs to JSON strings
	job1JSON, _ := json.Marshal(job1)
	job2JSON, _ := json.Marshal(job2)

	tests := []struct {
		name        string
		lines       []string
		wantJobs    []common.AsyncJob
		wantErr     bool
		errContains string
	}{
		{
			name:     "Single valid job",
			lines:    []string{string(job1JSON)},
			wantJobs: []common.AsyncJob{job1},
			wantErr:  false,
		},
		{
			name:     "Multiple valid jobs",
			lines:    []string{string(job1JSON), string(job2JSON)},
			wantJobs: []common.AsyncJob{job1, job2},
			wantErr:  false,
		},
		{
			name:     "Empty file",
			lines:    []string{},
			wantJobs: []common.AsyncJob{},
			wantErr:  false,
		},
		{
			name: "File with empty lines",
			lines: []string{
				string(job1JSON),
				"",
				string(job2JSON),
				"",
			},
			wantErr:     true,
			errContains: "BRT: Error in Unmarshalling Job: readObjectStart: expect { or n, but found \x00, error found in #0 byte of ...||..., bigger context ...||...",
		},
		{
			name: "Invalid JSON in line",
			lines: []string{
				string(job1JSON),
				`{"invalid json`,
			},
			wantErr:     true,
			errContains: "BRT: Error in Unmarshalling Job: common.AsyncJob.readFieldHash: incomplete field name, error found in #10 byte of ...|valid json|..., bigger context ...|{\"invalid json|...",
		},
		{
			name: "Line with non-JSON content",
			lines: []string{
				string(job1JSON),
				"This is not JSON at all",
			},
			wantErr:     true,
			errContains: "BRT: Error in Unmarshalling Job: readObjectStart: expect { or n, but found T, error found in #1 byte of ...|This is not|..., bigger context ...|This is not JSON at all|...",
		},
		{
			name: "Line with array instead of object",
			lines: []string{
				string(job1JSON),
				"[1, 2, 3]",
			},
			wantErr:     true,
			errContains: "BRT: Error in Unmarshalling Job: readObjectStart: expect { or n, but found [, error found in #1 byte of ...|[1, 2, 3]|..., bigger context ...|[1, 2, 3]|...",
		},
		{
			name: "Line with extra whitespace",
			lines: []string{
				string(job1JSON) + "    ",
				"   " + string(job2JSON),
			},
			wantJobs: []common.AsyncJob{job1, job2},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test file with content
			filePath := createTestFile(tt.lines)

			// Run the test
			gotJobs, err := readJobsFromFile(filePath)

			// Check error conditions
			if tt.wantErr {
				if err == nil {
					t.Error("readJobsFromFile() expected error but got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("readJobsFromFile() error = %v, want error containing %v", err, tt.errContains)
					return
				}
			} else {
				if err != nil {
					t.Errorf("readJobsFromFile() unexpected error = %v", err)
					return
				}

				// Compare results
				if !reflect.DeepEqual(gotJobs, tt.wantJobs) {
					t.Errorf("readJobsFromFile() = %v, want %v", gotJobs, tt.wantJobs)
				}
			}
		})
	}

	// 	// Test file system errors
	t.Run("Non-existent file", func(t *testing.T) {
		nonExistentPath := filepath.Join(tmpDir, "non_existent_file.txt")
		_, err := readJobsFromFile(nonExistentPath)
		if err == nil {
			t.Error("readJobsFromFile() expected error for non-existent file but got nil")
		}
		if !strings.Contains(err.Error(), "Read File Failed") {
			t.Errorf("readJobsFromFile() error = %v, want error containing 'Read File Failed'", err)
		}
	})

	t.Run("No read permissions", func(t *testing.T) {
		filePath := createTestFile([]string{string(job1JSON)})
		if err := os.Chmod(filePath, 0o000); err != nil {
			t.Fatalf("Failed to change file permissions: %v", err)
		}
		_, err := readJobsFromFile(filePath)
		if err == nil {
			t.Error("readJobsFromFile() expected error for no read permissions but got nil")
		}
		if !strings.Contains(err.Error(), "Read File Failed") {
			t.Errorf("readJobsFromFile() error = %v, want error containing 'Read File Failed'", err)
		}
	})

	// Test very large file simulation
	t.Run("Large file with many jobs", func(t *testing.T) {
		var lines []string
		for i := 0; i < 1000; i++ {
			job := common.AsyncJob{
				Message: map[string]interface{}{
					"email": fmt.Sprintf("test%d@example.com", i),
					"name":  fmt.Sprintf("Test User %d", i),
				},
				Metadata: map[string]interface{}{
					"job_id": float64(i),
				},
			}
			jobJSON, _ := json.Marshal(job)
			lines = append(lines, string(jobJSON))
		}
		filePath := createTestFile(lines)

		gotJobs, err := readJobsFromFile(filePath)
		if err != nil {
			t.Errorf("readJobsFromFile() unexpected error = %v", err)
			return
		}
		if len(gotJobs) != 1000 {
			t.Errorf("readJobsFromFile() got %d jobs, want 1000", len(gotJobs))
		}
	})
}

// ==== CSV File Parsing Test End ====

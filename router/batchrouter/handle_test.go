package batchrouter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/filemanager/mock_filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/timeutil"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
)

// TestGenerateSchemaMapCornerCases tests various corner cases for the generateSchemaMap function
func TestGenerateSchemaMap(t *testing.T) {
	testCases := []struct {
		name      string
		setupJobs func() []*jobsdb.JobT
		expected  map[string]map[string]string
	}{
		{
			name: "Empty Jobs List",
			setupJobs: func() []*jobsdb.JobT {
				return []*jobsdb.JobT{}
			},
			expected: map[string]map[string]string{},
		},
		{
			name: "Jobs With No Table Name",
			setupJobs: func() []*jobsdb.JobT {
				jobs := make([]*jobsdb.JobT, 3)
				for i := 0; i < 3; i++ {
					// Create metadata without table name
					metadata := map[string]interface{}{
						"columns": map[string]string{
							"column_1": "string",
						},
					}
					eventPayload := map[string]interface{}{
						"metadata": metadata,
					}
					payload, _ := jsonrs.Marshal(eventPayload)
					jobs[i] = &jobsdb.JobT{
						JobID:        int64(i + 1),
						EventPayload: payload,
					}
				}
				return jobs
			},
			expected: map[string]map[string]string{},
		},
		{
			name: "Jobs With Empty Columns",
			setupJobs: func() []*jobsdb.JobT {
				jobs := make([]*jobsdb.JobT, 3)
				for i := 0; i < 3; i++ {
					metadata := map[string]interface{}{
						"table":   "table_1",
						"columns": map[string]string{},
					}
					eventPayload := map[string]interface{}{
						"metadata": metadata,
					}
					payload, _ := jsonrs.Marshal(eventPayload)
					jobs[i] = &jobsdb.JobT{
						JobID:        int64(i + 1),
						EventPayload: payload,
					}
				}
				return jobs
			},
			expected: map[string]map[string]string{
				"table_1": {},
			},
		},
		{
			name: "Type Precedence - Text Over String",
			setupJobs: func() []*jobsdb.JobT {
				jobs := make([]*jobsdb.JobT, 2)

				// First job with string type
				metadata1 := map[string]interface{}{
					"table": "table_1",
					"columns": map[string]string{
						"column_1": "string",
					},
				}
				eventPayload1 := map[string]interface{}{
					"metadata": metadata1,
				}
				payload1, _ := jsonrs.Marshal(eventPayload1)
				jobs[0] = &jobsdb.JobT{
					JobID:        1,
					EventPayload: payload1,
				}

				// Second job with text type for same column
				metadata2 := map[string]interface{}{
					"table": "table_1",
					"columns": map[string]string{
						"column_1": "text",
					},
				}
				eventPayload2 := map[string]interface{}{
					"metadata": metadata2,
				}
				payload2, _ := jsonrs.Marshal(eventPayload2)
				jobs[1] = &jobsdb.JobT{
					JobID:        2,
					EventPayload: payload2,
				}

				return jobs
			},
			expected: map[string]map[string]string{
				"table_1": {
					"column_1": "text",
				},
			},
		},
		{
			name: "Type Precedence - String Not Overriding Text",
			setupJobs: func() []*jobsdb.JobT {
				jobs := make([]*jobsdb.JobT, 2)

				// First job with text type
				metadata1 := map[string]interface{}{
					"table": "table_1",
					"columns": map[string]string{
						"column_1": "text",
					},
				}
				eventPayload1 := map[string]interface{}{
					"metadata": metadata1,
				}
				payload1, _ := jsonrs.Marshal(eventPayload1)
				jobs[0] = &jobsdb.JobT{
					JobID:        1,
					EventPayload: payload1,
				}

				// Second job with string type for same column
				metadata2 := map[string]interface{}{
					"table": "table_1",
					"columns": map[string]string{
						"column_1": "string",
					},
				}
				eventPayload2 := map[string]interface{}{
					"metadata": metadata2,
				}
				payload2, _ := jsonrs.Marshal(eventPayload2)
				jobs[1] = &jobsdb.JobT{
					JobID:        2,
					EventPayload: payload2,
				}

				return jobs
			},
			expected: map[string]map[string]string{
				"table_1": {
					"column_1": "text",
				},
			},
		},
		{
			name: "Multiple Tables With Overlapping Column Names",
			setupJobs: func() []*jobsdb.JobT {
				jobs := make([]*jobsdb.JobT, 2)

				// Job for table_1
				metadata1 := map[string]interface{}{
					"table": "table_1",
					"columns": map[string]string{
						"column_1": "string",
						"column_2": "int",
					},
				}
				eventPayload1 := map[string]interface{}{
					"metadata": metadata1,
				}
				payload1, _ := jsonrs.Marshal(eventPayload1)
				jobs[0] = &jobsdb.JobT{
					JobID:        1,
					EventPayload: payload1,
				}

				// Job for table_2 with some overlapping column names
				metadata2 := map[string]interface{}{
					"table": "table_2",
					"columns": map[string]string{
						"column_1": "text",  // Same name but different type
						"column_3": "float", // Unique to table_2
					},
				}
				eventPayload2 := map[string]interface{}{
					"metadata": metadata2,
				}
				payload2, _ := jsonrs.Marshal(eventPayload2)
				jobs[1] = &jobsdb.JobT{
					JobID:        2,
					EventPayload: payload2,
				}

				return jobs
			},
			expected: map[string]map[string]string{
				"table_1": {
					"column_1": "string",
					"column_2": "int",
				},
				"table_2": {
					"column_1": "text",
					"column_3": "float",
				},
			},
		},
		{
			name: "Mixed Valid and Invalid Jobs",
			setupJobs: func() []*jobsdb.JobT {
				jobs := make([]*jobsdb.JobT, 4)

				// Valid job 1
				metadata1 := map[string]interface{}{
					"table": "table_1",
					"columns": map[string]string{
						"column_1": "string",
					},
				}
				eventPayload1 := map[string]interface{}{
					"metadata": metadata1,
				}
				payload1, _ := jsonrs.Marshal(eventPayload1)
				jobs[0] = &jobsdb.JobT{
					JobID:        1,
					EventPayload: payload1,
				}

				// Invalid job - no table name
				metadata2 := map[string]interface{}{
					"columns": map[string]string{
						"column_1": "int",
					},
				}
				eventPayload2 := map[string]interface{}{
					"metadata": metadata2,
				}
				payload2, _ := jsonrs.Marshal(eventPayload2)
				jobs[1] = &jobsdb.JobT{
					JobID:        2,
					EventPayload: payload2,
				}

				// Valid job 2 - same table as job 1
				metadata3 := map[string]interface{}{
					"table": "table_1",
					"columns": map[string]string{
						"column_2": "float",
					},
				}
				eventPayload3 := map[string]interface{}{
					"metadata": metadata3,
				}
				payload3, _ := jsonrs.Marshal(eventPayload3)
				jobs[2] = &jobsdb.JobT{
					JobID:        3,
					EventPayload: payload3,
				}

				// Invalid job - empty payload
				jobs[3] = &jobsdb.JobT{
					JobID:        4,
					EventPayload: []byte{},
				}

				return jobs
			},
			expected: map[string]map[string]string{
				"table_1": {
					"column_1": "string",
					"column_2": "float",
				},
			},
		},
		{
			name: "Many Tables With Few Jobs Each",
			setupJobs: func() []*jobsdb.JobT {
				jobs := make([]*jobsdb.JobT, 10)

				// Create 10 jobs with 10 different tables
				for i := 0; i < 10; i++ {
					tableName := fmt.Sprintf("table_%d", i)
					metadata := map[string]interface{}{
						"table": tableName,
						"columns": map[string]string{
							"column_1": "string",
							"column_2": "int",
						},
					}
					eventPayload := map[string]interface{}{
						"metadata": metadata,
					}
					payload, _ := jsonrs.Marshal(eventPayload)
					jobs[i] = &jobsdb.JobT{
						JobID:        int64(i + 1),
						EventPayload: payload,
					}
				}

				return jobs
			},
			expected: func() map[string]map[string]string {
				result := make(map[string]map[string]string)
				for i := 0; i < 10; i++ {
					tableName := fmt.Sprintf("table_%d", i)
					result[tableName] = map[string]string{
						"column_1": "string",
						"column_2": "int",
					}
				}
				return result
			}(),
		},
		{
			name: "Few Tables With Many Jobs Each",
			setupJobs: func() []*jobsdb.JobT {
				jobs := make([]*jobsdb.JobT, 20)

				// Create 20 jobs with only 2 different tables
				for i := 0; i < 20; i++ {
					tableIndex := i % 2
					tableName := fmt.Sprintf("table_%d", tableIndex)

					// Add some variation in columns based on job index
					columns := map[string]string{
						"column_1": "string",
						"column_2": "int",
					}

					// Every third job adds an extra column
					if i%3 == 0 {
						columns[fmt.Sprintf("extra_column_%d", i)] = "float"
					}

					metadata := map[string]interface{}{
						"table":   tableName,
						"columns": columns,
					}
					eventPayload := map[string]interface{}{
						"metadata": metadata,
					}
					payload, _ := jsonrs.Marshal(eventPayload)
					jobs[i] = &jobsdb.JobT{
						JobID:        int64(i + 1),
						EventPayload: payload,
					}
				}

				return jobs
			},
			expected: map[string]map[string]string{
				"table_0": {
					"column_1":        "string",
					"column_2":        "int",
					"extra_column_0":  "float",
					"extra_column_6":  "float",
					"extra_column_12": "float",
					"extra_column_18": "float",
				},
				"table_1": {
					"column_1":        "string",
					"column_2":        "int",
					"extra_column_3":  "float",
					"extra_column_9":  "float",
					"extra_column_15": "float",
				},
			},
		},
		{
			name: "Nil EventPayload",
			setupJobs: func() []*jobsdb.JobT {
				jobs := make([]*jobsdb.JobT, 2)

				// Valid job
				metadata1 := map[string]interface{}{
					"table": "table_1",
					"columns": map[string]string{
						"column_1": "string",
					},
				}
				eventPayload1 := map[string]interface{}{
					"metadata": metadata1,
				}
				payload1, _ := jsonrs.Marshal(eventPayload1)
				jobs[0] = &jobsdb.JobT{
					JobID:        1,
					EventPayload: payload1,
				}

				// Job with nil EventPayload
				jobs[1] = &jobsdb.JobT{
					JobID:        2,
					EventPayload: nil,
				}

				return jobs
			},
			expected: map[string]map[string]string{
				"table_1": {
					"column_1": "string",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			brt := &Handle{
				schemaGenerationWorkers: config.GetReloadableIntVar(4, 1, "BatchRouter.processingWorkers"),
			}

			jobs := tc.setupJobs()
			batchJobs := &BatchedJobs{Jobs: jobs}

			// Test the current implementation in handle.go
			result := brt.generateSchemaMap(batchJobs)

			// Verify results against expected
			require.Equal(t, tc.expected, result, "generateSchemaMap failed for case: %s", tc.name)
		})
	}
}

type testCase struct {
	name               string
	jobs               []*jobsdb.JobT
	expectedTableBytes map[string]int64
	expectedTotalBytes int
	isWarehouse        bool
}

func TestBytesPerTable(t *testing.T) {
	newHandle := func(isWarehouse bool) *Handle {
		mockCtrl := gomock.NewController(t)
		mockFileManager := mock_filemanager.NewMockFileManager(mockCtrl)
		mockFileManagerFactory := func(settings *filemanager.Settings) (filemanager.FileManager, error) { return mockFileManager, nil }
		mockFileManager.EXPECT().Prefix().Return("mockPrefix")
		mockFileObjects := []*filemanager.FileInfo{}
		mockFileManager.EXPECT().ListFilesWithPrefix(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(filemanager.MockListSession(mockFileObjects, nil))
		mockFileManager.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).Return(filemanager.UploadedFile{Location: "local", ObjectName: "file"}, nil)
		jobsDB := mocksJobsDB.NewMockJobsDB(mockCtrl)
		if !isWarehouse {
			jobsDB.EXPECT().JournalMarkStart(gomock.Any(), gomock.Any()).Times(1).Return(int64(1), nil)
		}
		return &Handle{
			logger:                  logger.NewLogger().Child("batchrouter"),
			fileManagerFactory:      mockFileManagerFactory,
			datePrefixOverride:      config.GetReloadableStringVar("", "BatchRouter.datePrefixOverride"),
			customDatePrefix:        config.GetReloadableStringVar("", "BatchRouter.customDatePrefix"),
			dateFormatProvider:      &storageDateFormatProvider{dateFormatsCache: make(map[string]string)},
			conf:                    config.New(),
			now:                     timeutil.Now,
			jobsDB:                  jobsDB,
			useAWSV2:                config.GetReloadableBoolVar(false, "BatchRouter.useAWSV2"),
			useDigitalOceanSpacesV2: config.GetReloadableBoolVar(false, "BatchRouter.useDigitalOceanSpacesV2"),
		}
	}

	tests := []testCase{
		{
			name: "single table warehouse upload",
			jobs: []*jobsdb.JobT{
				{
					EventPayload: []byte(`{
						"metadata": {
							"table": "users",
						},
					}`),
				},
				{
					EventPayload: []byte(`{
						"metadata": {
							"table": "users",
						},
					}`),
				},
			},
			expectedTableBytes: map[string]int64{
				"users": 126,
			},
			expectedTotalBytes: 126,
			isWarehouse:        true,
		},
		{
			name: "multiple tables warehouse upload",
			jobs: []*jobsdb.JobT{
				{
					EventPayload: []byte(`{
						"metadata": {
							"table": "users1",
						},
					}`),
				},
				{
					EventPayload: []byte(`{
						"metadata": {
							"table": "users2",
						},
					}`),
				},
				{
					EventPayload: []byte(`{
						"metadata": {
							"table": "users1",
						},
					}`),
				},
				{
					EventPayload: []byte(`{
						"metadata": {
							"table": "users3",
						},
					}`),
				},
				{
					EventPayload: []byte(`{}`),
				},
			},
			expectedTableBytes: map[string]int64{
				"users1": 128,
				"users2": 64,
				"users3": 64,
				"":       3,
			},
			expectedTotalBytes: 259,
			isWarehouse:        true,
		},
		{
			name: "non-warehouse upload",
			jobs: []*jobsdb.JobT{
				{
					EventPayload: []byte(`{
						"metadata": {
							"table": "users",
						},
					}`),
				},
			},
			expectedTotalBytes: 63,
			isWarehouse:        false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			batchedJobs := &BatchedJobs{
				Jobs: tc.jobs,
				Connection: &Connection{
					Source: backendconfig.SourceT{
						ID: "test-source",
					},
					Destination: backendconfig.DestinationT{
						ID: "test-destination",
					},
				},
			}
			handle := newHandle(tc.isWarehouse)
			result := handle.upload("S3", batchedJobs, tc.isWarehouse)

			// Verify results
			require.Equal(t, tc.expectedTotalBytes, result.TotalBytes, "total bytes mismatch")
			if tc.isWarehouse {
				require.Equal(t, tc.expectedTableBytes, result.BytesPerTable, "bytes per table mismatch")
			} else {
				require.Len(t, result.BytesPerTable, 0)
			}
		})
	}
}

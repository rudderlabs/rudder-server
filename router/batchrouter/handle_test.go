package batchrouter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/jsonrs"
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

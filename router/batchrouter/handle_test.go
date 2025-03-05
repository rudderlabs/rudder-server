package batchrouter

import (
	"fmt"
	"testing"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/jsonrs"
	"github.com/tidwall/gjson"
)

func BaseLine(batchJobs *BatchedJobs) {
	schemaMap := make(map[string]map[string]interface{})
	for _, job := range batchJobs.Jobs {
		metadata := gjson.GetBytes(job.EventPayload, "metadata").Map()
		tableName := metadata["table"].String()
		if tableName == "" {
			continue
		}
		if _, ok := schemaMap[tableName]; !ok {
			schemaMap[tableName] = make(map[string]interface{})
		}
		columns := metadata["columns"].Map()
		for columnName, columnType := range columns {
			if _, ok := schemaMap[tableName][columnName]; !ok {
				schemaMap[tableName][columnName] = columnType
			} else if columnType.String() == "text" && schemaMap[tableName][columnName] == "string" {
				// this condition is required for altering string to text. if schemaMap[tableName][columnName] has string and in the next job if it has text type then we change schemaMap[tableName][columnName] to text
				schemaMap[tableName][columnName] = columnType
			}
		}
	}
	return
}

func OldSchemaMap(batchJobs *BatchedJobs) {
	schemaMap := make(map[string]map[string]interface{})
	for _, job := range batchJobs.Jobs {
		var payload map[string]interface{}
		err := jsonrs.Unmarshal(job.EventPayload, &payload)
		if err != nil {
			continue
		}
		var ok bool
		tableName, ok := payload["metadata"].(map[string]interface{})["table"].(string)
		if !ok {
			continue
		}
		if _, ok = schemaMap[tableName]; !ok {
			schemaMap[tableName] = make(map[string]interface{})
		}
		columns := payload["metadata"].(map[string]interface{})["columns"].(map[string]interface{})
		for columnName, columnType := range columns {
			if _, ok := schemaMap[tableName][columnName]; !ok {
				schemaMap[tableName][columnName] = columnType
			} else if columnType == "text" && schemaMap[tableName][columnName] == "string" {
				// this condition is required for altering string to text. if schemaMap[tableName][columnName] has string and in the next job if it has text type then we change schemaMap[tableName][columnName] to text
				schemaMap[tableName][columnName] = columnType
			}
		}
	}
	return
}

// Generate a sample EventPayload with many more tables and columns
// This is to test the performance of the BaseLine and OldSchemaMap functions
// The payload is also supposed to have a metadata with table and columns
func generateEventPayload(count int) []byte {
	payload := make(map[string]interface{})
	tableNum := count % 10         // cycle through 10 different tables
	columnCount := (count % 5) + 2 // vary number of columns between 2-6

	columns := make(map[string]interface{})
	columns["id"] = "string"
	columns["name"] = func() interface{} {
		if count%3 == 0 { // occasionally make it "text" instead of "string"
			return "text"
		}
		return "string"
	}()

	// Add some varying columns
	for i := 0; i < columnCount; i++ {
		columns[fmt.Sprintf("col_%d", i)] = func() interface{} {
			switch i % 3 {
			case 0:
				return "string"
			case 1:
				return "text"
			default:
				return "integer"
			}
		}()
	}

	payload["metadata"] = map[string]interface{}{
		"table":   fmt.Sprintf("table%d", tableNum),
		"columns": columns,
	}
	res, _ := jsonrs.Marshal(payload)
	return res
}

func generateBatchJobs(count int) *BatchedJobs {
	batchJobs := &BatchedJobs{}
	for i := 0; i < count; i++ {
		batchJobs.Jobs = append(batchJobs.Jobs, &jobsdb.JobT{EventPayload: generateEventPayload(i)})
	}
	return batchJobs
}

//BenchmarkBaseLine
//BenchmarkBaseLine/jobsCount=100
//BenchmarkBaseLine/jobsCount=100-12         	   13258	     92678 ns/op
//BenchmarkBaseLine/jobsCount=1000
//BenchmarkBaseLine/jobsCount=1000-12        	    1365	    880910 ns/op
//BenchmarkBaseLine/jobsCount=10000
//BenchmarkBaseLine/jobsCount=10000-12       	     129	   9197343 ns/op
//BenchmarkBaseLine/jobsCount=100000
//BenchmarkBaseLine/jobsCount=100000-12      	      12	  86795309 ns/op

func BenchmarkBaseLine(b *testing.B) {
	jobsCount := []int{100, 1000, 10000, 100000}
	for _, count := range jobsCount {
		batchJobs := generateBatchJobs(count)
		b.Run(fmt.Sprintf("jobsCount=%d", count), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				BaseLine(batchJobs)
			}
		})
	}
}

//BenchmarkOldSchemaMap
//BenchmarkOldSchemaMap/jobsCount=100
//BenchmarkOldSchemaMap/jobsCount=100-12         	    8232	    130948 ns/op
//BenchmarkOldSchemaMap/jobsCount=1000
//BenchmarkOldSchemaMap/jobsCount=1000-12        	     912	   1290493 ns/op
//BenchmarkOldSchemaMap/jobsCount=10000
//BenchmarkOldSchemaMap/jobsCount=10000-12       	      92	  12730642 ns/op
//BenchmarkOldSchemaMap/jobsCount=100000
//BenchmarkOldSchemaMap/jobsCount=100000-12      	       9	 126674972 ns/op

func BenchmarkOldSchemaMap(b *testing.B) {
	jobsCount := []int{100, 1000, 10000, 100000}
	for _, count := range jobsCount {
		batchJobs := generateBatchJobs(count)
		b.Run(fmt.Sprintf("jobsCount=%d", count), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				OldSchemaMap(batchJobs)
			}
		})
	}
}

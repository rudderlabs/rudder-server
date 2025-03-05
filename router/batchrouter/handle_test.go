package batchrouter

import (
	"fmt"
	"sync"
	"testing"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/jsonrs"
)

func ParallelSchemaMapGeneration(batchJobs *BatchedJobs, workers int) {
	schemaMap := make(map[string]map[string]interface{})
	var schemaMapMu sync.Mutex // Mutex to protect concurrent map access

	// Process jobs in parallel using worker pool
	maxWorkers := workers
	jobsChan := make(chan *jobsdb.JobT, len(batchJobs.Jobs))
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobsChan {
				metadata := gjson.GetBytes(job.EventPayload, "metadata").Map()
				tableName := metadata["table"].String()
				if tableName == "" {
					continue
				}

				// Create local schema map for this job
				localSchema := make(map[string]map[string]interface{})
				if _, ok := localSchema[tableName]; !ok {
					localSchema[tableName] = make(map[string]interface{})
				}

				columns := metadata["columns"].Map()
				for columnName, columnType := range columns {
					localSchema[tableName][columnName] = columnType
				}

				// Merge local schema into global schema with mutex protection
				schemaMapMu.Lock()
				if _, ok := schemaMap[tableName]; !ok {
					schemaMap[tableName] = make(map[string]interface{})
				}
				for columnName, columnType := range localSchema[tableName] {
					if existingType, ok := schemaMap[tableName][columnName]; !ok {
						schemaMap[tableName][columnName] = columnType
					} else if columnType.(gjson.Result).String() == "text" && existingType == "string" {
						schemaMap[tableName][columnName] = columnType
					}
				}
				schemaMapMu.Unlock()
			}
		}()
	}

	// Send jobs to workers
	for _, job := range batchJobs.Jobs {
		jobsChan <- job
	}
	close(jobsChan)

	// Wait for all workers to complete
	wg.Wait()
}

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

// BenchmarkBaseLine
// BenchmarkBaseLine/jobsCount=100
// BenchmarkBaseLine/jobsCount=100-12         	   13258	     92678 ns/op
// BenchmarkBaseLine/jobsCount=1000
// BenchmarkBaseLine/jobsCount=1000-12        	    1365	    880910 ns/op
// BenchmarkBaseLine/jobsCount=10000
// BenchmarkBaseLine/jobsCount=10000-12       	     129	   9197343 ns/op
// BenchmarkBaseLine/jobsCount=100000
// BenchmarkBaseLine/jobsCount=100000-12      	      12	  86795309 ns/op
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

// BenchmarkOldSchemaMap
// BenchmarkOldSchemaMap/jobsCount=100
// BenchmarkOldSchemaMap/jobsCount=100-12         	    8232	    130948 ns/op
// BenchmarkOldSchemaMap/jobsCount=1000
// BenchmarkOldSchemaMap/jobsCount=1000-12        	     912	   1290493 ns/op
// BenchmarkOldSchemaMap/jobsCount=10000
// BenchmarkOldSchemaMap/jobsCount=10000-12       	      92	  12730642 ns/op
// BenchmarkOldSchemaMap/jobsCount=100000
// BenchmarkOldSchemaMap/jobsCount=100000-12      	       9	 126674972 ns/op
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

// BenchmarkParallelSchemaMapGeneration
// BenchmarkParallelSchemaMapGeneration/jobsCount=100,workers=10
// BenchmarkParallelSchemaMapGeneration/jobsCount=100,workers=10-12         	   10000	    128204 ns/op
// BenchmarkParallelSchemaMapGeneration/jobsCount=100,workers=20
// BenchmarkParallelSchemaMapGeneration/jobsCount=100,workers=20-12         	   10000	    117502 ns/op
// BenchmarkParallelSchemaMapGeneration/jobsCount=100,workers=40
// BenchmarkParallelSchemaMapGeneration/jobsCount=100,workers=40-12         	   10000	    117866 ns/op
// BenchmarkParallelSchemaMapGeneration/jobsCount=100,workers=80
// BenchmarkParallelSchemaMapGeneration/jobsCount=100,workers=80-12         	    9591	    133180 ns/op
// BenchmarkParallelSchemaMapGeneration/jobsCount=1000,workers=10
// BenchmarkParallelSchemaMapGeneration/jobsCount=1000,workers=10-12        	    1324	    885033 ns/op
// BenchmarkParallelSchemaMapGeneration/jobsCount=1000,workers=20
// BenchmarkParallelSchemaMapGeneration/jobsCount=1000,workers=20-12        	    1248	    908129 ns/op
// BenchmarkParallelSchemaMapGeneration/jobsCount=1000,workers=40
// BenchmarkParallelSchemaMapGeneration/jobsCount=1000,workers=40-12        	    1300	    915411 ns/op
// BenchmarkParallelSchemaMapGeneration/jobsCount=1000,workers=80
// BenchmarkParallelSchemaMapGeneration/jobsCount=1000,workers=80-12        	    1292	    948777 ns/op
// BenchmarkParallelSchemaMapGeneration/jobsCount=10000,workers=10
// BenchmarkParallelSchemaMapGeneration/jobsCount=10000,workers=10-12       	     142	   8412655 ns/op
// BenchmarkParallelSchemaMapGeneration/jobsCount=10000,workers=20
// BenchmarkParallelSchemaMapGeneration/jobsCount=10000,workers=20-12       	     139	   8550822 ns/op
// BenchmarkParallelSchemaMapGeneration/jobsCount=10000,workers=40
// BenchmarkParallelSchemaMapGeneration/jobsCount=10000,workers=40-12       	     139	   8603418 ns/op
// BenchmarkParallelSchemaMapGeneration/jobsCount=10000,workers=80
// BenchmarkParallelSchemaMapGeneration/jobsCount=10000,workers=80-12       	     138	   8596863 ns/op
// BenchmarkParallelSchemaMapGeneration/jobsCount=100000,workers=10
// BenchmarkParallelSchemaMapGeneration/jobsCount=100000,workers=10-12      	      14	  78992500 ns/op
// BenchmarkParallelSchemaMapGeneration/jobsCount=100000,workers=20
// BenchmarkParallelSchemaMapGeneration/jobsCount=100000,workers=20-12      	      14	  80797381 ns/op
// BenchmarkParallelSchemaMapGeneration/jobsCount=100000,workers=40
// BenchmarkParallelSchemaMapGeneration/jobsCount=100000,workers=40-12      	      14	  81011595 ns/op
// BenchmarkParallelSchemaMapGeneration/jobsCount=100000,workers=80
// BenchmarkParallelSchemaMapGeneration/jobsCount=100000,workers=80-12      	      14	  80271583 ns/op
func BenchmarkParallelSchemaMapGeneration(b *testing.B) {
	jobsCount := []int{100, 1000, 10000, 100000}
	for _, count := range jobsCount {
		batchJobs := generateBatchJobs(count)
		workers := []int{10, 20, 40, 80}
		for _, worker := range workers {
			b.Run(fmt.Sprintf("jobsCount=%d,workers=%d", count, worker), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					ParallelSchemaMapGeneration(batchJobs, worker)
				}
			})
		}
	}
}

func OptimizedSchemaMapGenerationTest(batchJobs *BatchedJobs, workers int) map[string]map[string]interface{} {
	// Split jobs into chunks for each worker
	chunkSize := (len(batchJobs.Jobs) + workers - 1) / workers
	chunks := make([][]*jobsdb.JobT, workers)
	activeWorkers := 0 // Track actual number of workers with data

	for i := 0; i < workers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(batchJobs.Jobs) {
			end = len(batchJobs.Jobs)
		}
		if start < len(batchJobs.Jobs) {
			chunks[i] = batchJobs.Jobs[start:end]
			activeWorkers++
		}
	}

	// Create buffered channel with exact size needed
	results := make(chan workerResult, activeWorkers)

	// Create slice to store all results to ensure nothing is lost
	allResults := make([]workerResult, 0, activeWorkers)

	var wg sync.WaitGroup

	// Launch only the workers that have data
	for i := 0; i < workers; i++ {
		if len(chunks[i]) == 0 {
			continue
		}
		wg.Add(1)
		go func(chunk []*jobsdb.JobT, idx int) {
			defer wg.Done()

			// Each worker builds its own schema map independently
			localSchema := make(map[string]map[string]interface{})

			for _, job := range chunk {
				metadata := gjson.GetBytes(job.EventPayload, "metadata").Map()
				tableName := metadata["table"].String()
				if tableName == "" {
					continue
				}

				if _, ok := localSchema[tableName]; !ok {
					localSchema[tableName] = make(map[string]interface{})
				}

				columns := metadata["columns"].Map()
				for columnName, columnType := range columns {
					if existingType, ok := localSchema[tableName][columnName]; !ok {
						localSchema[tableName][columnName] = columnType
					} else if columnType.String() == "text" && existingType == "string" {
						localSchema[tableName][columnName] = columnType
					}
				}
			}
			results <- workerResult{schema: localSchema, index: idx}
		}(chunks[i], i)
	}

	// Use a separate goroutine to collect results
	var collectWg sync.WaitGroup
	collectWg.Add(1)

	go func() {
		defer collectWg.Done()
		// Collect exactly activeWorkers results
		for i := 0; i < activeWorkers; i++ {
			result := <-results
			allResults = append(allResults, result)
		}
	}()

	// Wait for all workers to finish
	wg.Wait()
	// Wait for result collection to complete
	collectWg.Wait()

	// Verify we got all results
	if len(allResults) != activeWorkers {
		panic(fmt.Sprintf("Critical error: Expected %d results but got %d", activeWorkers, len(allResults)))
	}

	// Merge all results into final schema
	finalSchema := make(map[string]map[string]interface{})
	for _, result := range allResults {
		for tableName, columns := range result.schema {
			if _, ok := finalSchema[tableName]; !ok {
				finalSchema[tableName] = make(map[string]interface{})
			}
			for columnName, columnType := range columns {
				if existingType, ok := finalSchema[tableName][columnName]; !ok {
					finalSchema[tableName][columnName] = columnType
				} else if columnType.(gjson.Result).String() == "text" && existingType == "string" {
					finalSchema[tableName][columnName] = columnType
				}
			}
		}
	}

	return finalSchema
}

// BenchmarkOptimizedSchemaMapGeneration benchmarks the optimized version with different job counts and worker counts
func BenchmarkOptimizedSchemaMapGeneration(b *testing.B) {
	jobsCount := []int{100, 1000, 10000, 100000}
	workers := []int{4, 8, 16}

	for _, count := range jobsCount {
		batchJobs := generateBatchJobs(count)
		for _, worker := range workers {
			b.Run(fmt.Sprintf("jobsCount=%d,workers=%d", count, worker), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					OptimizedSchemaMapGenerationTest(batchJobs, worker)
				}
			})
		}
	}
}

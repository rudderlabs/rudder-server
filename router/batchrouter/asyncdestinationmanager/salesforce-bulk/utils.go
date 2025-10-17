package salesforcebulk

import (
	"bufio"
	"crypto/sha256"
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

const (
	maxFileSize = 100 * 1024 * 1024
)

type ObjectInfo struct {
	ObjectType      string
	ExternalIDField string
}

func readJobsFromFile(filePath string) ([]common.AsyncJob, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}
	defer file.Close()

	var jobs []common.AsyncJob
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		var job common.AsyncJob
		if err := jsonrs.Unmarshal(scanner.Bytes(), &job); err != nil {
			return nil, fmt.Errorf("unmarshalling job: %w", err)
		}
		jobs = append(jobs, job)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanning file: %w", err)
	}

	return jobs, nil
}

func extractObjectInfo(jobs []common.AsyncJob, config DestinationConfig) (*ObjectInfo, error) {
	if len(jobs) == 0 {
		return nil, fmt.Errorf("no jobs to process")
	}

	firstJob := jobs[0]

	if externalIDRaw, ok := firstJob.Metadata["externalId"]; ok {
		return extractFromVDM(externalIDRaw)
	}

	objectType := config.ObjectType
	if objectType == "" {
		objectType = "Lead"
	}

	return &ObjectInfo{
		ObjectType:      objectType,
		ExternalIDField: "Email",
	}, nil
}

func extractFromVDM(externalIDRaw interface{}) (*ObjectInfo, error) {
	externalIDArray, ok := externalIDRaw.([]interface{})
	if !ok || len(externalIDArray) == 0 {
		return nil, fmt.Errorf("externalId must be an array with at least one element")
	}

	externalIDMap, ok := externalIDArray[0].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("externalId[0] must be an object")
	}

	typeStr, _ := externalIDMap["type"].(string)
	if typeStr == "" {
		return nil, fmt.Errorf("externalId type is required")
	}

	objectType := strings.Replace(typeStr, "Salesforce-", "", 1)
	objectType = strings.Replace(objectType, "salesforce-", "", 1)

	identifierType, _ := externalIDMap["identifierType"].(string)

	return &ObjectInfo{
		ObjectType:      objectType,
		ExternalIDField: identifierType,
	}, nil
}

func createCSVFile(
	destinationID string,
	input []common.AsyncJob,
	dataHashToJobID map[string][]int64,
	operation string,
) (string, []string, []int64, []int64, error) {
	csvFilePath := fmt.Sprintf("/tmp/salesforce_%s_%d.csv", destinationID, time.Now().Unix())
	csvFile, err := os.Create(csvFilePath)
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("creating CSV file: %w", err)
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	var insertedJobIDs []int64
	var overflowedJobIDs []int64

	if len(input) == 0 {
		return "", nil, nil, nil, fmt.Errorf("no jobs to process")
	}

	headers := make([]string, 0, len(input[0].Message))
	for key := range input[0].Message {
		if key != "rudderOperation" {
			headers = append(headers, key)
		}
	}
	sort.Strings(headers)

	headerIndex := make(map[string]int)
	for i, key := range headers {
		headerIndex[key] = i
	}

	if err := writer.Write(headers); err != nil {
		return "", nil, nil, nil, fmt.Errorf("writing CSV header: %w", err)
	}

	currentSize := int64(len(strings.Join(headers, ",")) + 1)

	for _, job := range input {
		row := make([]string, len(headers))

		for key, value := range job.Message {
			if idx, ok := headerIndex[key]; ok {
				row[idx] = fmt.Sprintf("%v", value)
			}
		}

		rowSize := int64(len(strings.Join(row, ",")) + 1)
		jobID := int64(job.Metadata["job_id"].(float64))

		if currentSize+rowSize > maxFileSize {
			overflowedJobIDs = append(overflowedJobIDs, jobID)
			continue
		}

		if err := writer.Write(row); err != nil {
			return "", nil, nil, nil, fmt.Errorf("writing CSV row: %w", err)
		}

		currentSize += rowSize
		insertedJobIDs = append(insertedJobIDs, jobID)

		hash := calculateHashWithOperation(row, operation)
		dataHashToJobID[hash] = append(dataHashToJobID[hash], jobID)
	}

	return csvFilePath, headers, insertedJobIDs, overflowedJobIDs, nil
}

func calculateHashCode(row []string) string {
	joined := strings.Join(row, ",")
	hash := sha256.Sum256([]byte(joined))
	return fmt.Sprintf("%x", hash)
}

func calculateHashWithOperation(row []string, operation string) string {
	joined := strings.Join(append([]string{operation}, row...), ",")
	hash := sha256.Sum256([]byte(joined))
	return fmt.Sprintf("%x", hash)
}

func calculateHashFromRecord(record map[string]string, csvHeaders []string, operation string) string {
	values := make([]string, 0, len(csvHeaders))
	for _, header := range csvHeaders {
		values = append(values, record[header])
	}
	return calculateHashWithOperation(values, operation)
}

func extractOperationFromJob(job common.AsyncJob, defaultOperation string) string {
	if operation, ok := job.Message["rudderOperation"].(string); ok && operation != "" {
		return operation
	}
	return defaultOperation
}

func groupJobsByOperation(jobs []common.AsyncJob, defaultOperation string) map[string][]common.AsyncJob {
	grouped := make(map[string][]common.AsyncJob)

	for _, job := range jobs {
		operation := extractOperationFromJob(job, defaultOperation)
		grouped[operation] = append(grouped[operation], job)
	}

	return grouped
}

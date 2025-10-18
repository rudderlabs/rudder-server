package salesforcebulk

import (
	"bufio"
	"bytes"
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
	scanner.Buffer(nil, 50000*1024)

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

func extractFromVDM(externalIDRaw interface{}, message map[string]interface{}) (*ObjectInfo, error) {
	externalIDArray, ok := externalIDRaw.([]interface{})
	if !ok || len(externalIDArray) == 0 {
		return nil, fmt.Errorf("externalId must be an array with at least one element")
	}

	var firstEntry map[string]interface{}

	for idx, rawEntry := range externalIDArray {
		entry, ok := rawEntry.(map[string]interface{})
		if !ok {
			if idx == 0 {
				return nil, fmt.Errorf("externalId[0] must be an object")
			}
			return nil, fmt.Errorf("externalId[%d] must be an object", idx)
		}

		if firstEntry == nil {
			firstEntry = entry
		}

		typeStr, _ := entry["type"].(string)
		if typeStr == "" {
			return nil, fmt.Errorf("externalId type is required")
		}

		identifierType, _ := entry["identifierType"].(string)
		if identifierType == "" {
			identifierType = "Id"
		}

		idValue := ""
		if rawID, ok := entry["id"]; ok {
			idValue = fmt.Sprint(rawID)
		}

		if idValue != "" {
			if messageValue, ok := message[identifierType]; ok {
				if fmt.Sprint(messageValue) == idValue {
					return buildObjectInfo(typeStr, identifierType), nil
				}
			} else {
				return buildObjectInfo(typeStr, identifierType), nil
			}
		}

		if messageValue, ok := message[identifierType]; ok && fmt.Sprint(messageValue) != "" {
			return buildObjectInfo(typeStr, identifierType), nil
		}
	}

	if firstEntry == nil {
		return nil, fmt.Errorf("externalId must contain at least one object entry")
	}

	typeStr, _ := firstEntry["type"].(string)
	identifierType, _ := firstEntry["identifierType"].(string)
	if identifierType == "" {
		identifierType = "Id"
	}

	return buildObjectInfo(typeStr, identifierType), nil
}

func extractObjectInfoFromJob(job common.AsyncJob, config DestinationConfig) (*ObjectInfo, error) {
	if externalIDRaw, ok := job.Metadata["externalId"]; ok {
		return extractFromVDM(externalIDRaw, job.Message)
	}

	if externalIDRaw, ok := job.Message["externalId"]; ok {
		return extractFromVDM(externalIDRaw, job.Message)
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

func buildObjectInfo(typeStr, identifierType string) *ObjectInfo {
	objectType := strings.Replace(typeStr, "Salesforce-", "", 1)
	objectType = strings.Replace(objectType, "salesforce-", "", 1)

	return &ObjectInfo{
		ObjectType:      objectType,
		ExternalIDField: identifierType,
	}
}

func createCSVFile(
	destinationID string,
	input []common.AsyncJob,
	dataHashToJobID map[string][]int64,
	operation string,
) (string, []string, []int64, []common.AsyncJob, error) {
	csvFilePath := fmt.Sprintf("/tmp/salesforce_%s_%d.csv", destinationID, time.Now().Unix())
	csvFile, err := os.Create(csvFilePath)
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("creating CSV file: %w", err)
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	var insertedJobIDs []int64
	var overflowedJobs []common.AsyncJob

	if len(input) == 0 {
		return "", nil, nil, nil, fmt.Errorf("no jobs to process")
	}

	headerSet := make(map[string]bool)
	for _, job := range input {
		for key := range job.Message {
			if key != "rudderOperation" {
				headerSet[key] = true
			}
		}
	}

	headers := make([]string, 0, len(headerSet))
	for key := range headerSet {
		headers = append(headers, key)
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

		jobID := int64(job.Metadata["job_id"].(float64))

		buf := &bytes.Buffer{}
		tempWriter := csv.NewWriter(buf)
		if err := tempWriter.Write(row); err != nil {
			return "", nil, nil, nil, fmt.Errorf("calculating row size: %w", err)
		}
		tempWriter.Flush()
		rowSize := int64(buf.Len())

		if currentSize+rowSize > maxFileSize {
			overflowedJobs = append(overflowedJobs, job)
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

	return csvFilePath, headers, insertedJobIDs, overflowedJobs, nil
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

package salesforcebulkupload

import (
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/samber/lo"
)

const (
	maxFileSize = 100 * 1024 * 1024 // 100MB in bytes
)

func extractObjectInfo(jobs []common.AsyncJob) (*ObjectInfo, error) {
	if len(jobs) == 0 {
		return nil, fmt.Errorf("no jobs to process")
	}

	firstJob := jobs[0]

	externalIDRaw, ok := firstJob.Metadata["externalId"]
	if !ok {
		return nil, fmt.Errorf("externalId not found in the first job")
	}

	objectInfo, err := extractFromVDM(externalIDRaw)
	if err != nil {
		return nil, fmt.Errorf("failed to extract object info: %w", err)
	}
	return objectInfo, nil
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
	objectType := strings.Replace(typeStr, "SALESFORCE_BULK_UPLOAD-", "", 1)
	identifierType, _ := externalIDMap["identifierType"].(string)
	identifierValue, _ := externalIDMap["id"].(string)
	return &ObjectInfo{
		ObjectType:      objectType,
		ExternalIDField: identifierType,
		ExternalIDValue: identifierValue,
	}, nil
}

func createCSVFile(
	destinationID string,
	input []common.AsyncJob,
	dataHashToJobID map[string][]int64,
) (string, []string, []int64, []common.AsyncJob, error) {
	csvDir := "/tmp/rudder-async-destination-logs"
	if err := os.MkdirAll(csvDir, 0o755); err != nil {
		return "", nil, nil, nil, fmt.Errorf("creating CSV directory: %w", err)
	}

	csvFilePath := fmt.Sprintf("%s/salesforce_%s_%d.csv", csvDir, destinationID, time.Now().Unix())
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

	headerSet := make(map[string]struct{})
	for _, job := range input {
		for key := range job.Message {
			if _, ok := headerSet[key]; ok {
				continue
			}
			headerSet[key] = struct{}{}
		}
	}

	headers := lo.Keys(headerSet)

	sort.Strings(headers)

	headerIndex := make(map[string]int, len(headers))
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

		hash := calculateHashCode(row)
		dataHashToJobID[hash] = append(dataHashToJobID[hash], jobID)
	}

	return csvFilePath, headers, insertedJobIDs, overflowedJobs, nil
}

func calculateHashCode(row []string) string {
	joined := strings.Join(row, ",")
	hash := sha256.Sum256([]byte(joined))
	return fmt.Sprintf("%x", hash)
}

func calculateHashFromRecord(record map[string]string, csvHeaders []string) string {
	values := make([]string, 0, len(csvHeaders))
	for _, header := range csvHeaders {
		values = append(values, record[header])
	}
	return calculateHashCode(values)
}

func extractOperationFromJob(job common.AsyncJob, defaultOperation string) string {
	if operation, ok := job.Metadata["rudderOperation"].(string); ok && operation != "" {
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

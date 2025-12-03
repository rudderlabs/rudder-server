package salesforcebulkupload

import (
	"crypto/sha256"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
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
) (string, []string, map[string][]int64, error) {
	if err := os.MkdirAll(CSVDir, 0o755); err != nil {
		return "", nil, nil, fmt.Errorf("creating CSV directory: %w", err)
	}
	csvFilePath := filepath.Join(CSVDir, fmt.Sprintf("salesforce_%s_%d.csv", destinationID, time.Now().Unix()))
	csvFile, err := os.Create(csvFilePath)
	if err != nil {
		return "", nil, nil, fmt.Errorf("creating CSV file: %w", err)
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

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

	if err := writer.Write(headers); err != nil {
		return "", nil, nil, fmt.Errorf("writing CSV header: %w", err)
	}

	headerIndex := make(map[string]int, len(headers))
	for i, key := range headers {
		headerIndex[key] = i
	}

	dataHashToJobID := make(map[string][]int64)
	for _, job := range input {
		row := make([]string, len(headers))
		for key, value := range job.Message {
			if idx, ok := headerIndex[key]; ok {
				row[idx] = fmt.Sprintf("%v", value)
			}
		}
		jobID := int64(job.Metadata["job_id"].(float64))
		if err := writer.Write(row); err != nil {
			return "", nil, nil, fmt.Errorf("writing CSV row: %w", err)
		}
		hash := calculateHashCode(row)
		dataHashToJobID[hash] = append(dataHashToJobID[hash], jobID)
	}

	return csvFilePath, headers, dataHashToJobID, nil
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

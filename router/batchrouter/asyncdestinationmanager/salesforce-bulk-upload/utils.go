package salesforcebulkupload

import (
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

func extractFromVDM(externalIDRaw any) (*ObjectInfo, error) {
	externalIDArray, ok := externalIDRaw.([]any)
	if !ok || len(externalIDArray) == 0 {
		return nil, fmt.Errorf("externalId must be an array with at least one element")
	}

	externalIDMap, ok := externalIDArray[0].(map[string]any)
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
	externalIDField string,
	input []common.AsyncJob,
) (string, map[string][]int64, error) {
	if err := os.MkdirAll(CSVDir, 0o755); err != nil {
		return "", nil, fmt.Errorf("creating CSV directory: %w", err)
	}
	csvFilePath := filepath.Join(CSVDir, fmt.Sprintf("salesforce_%s_%d.csv", destinationID, time.Now().Unix()))
	csvFile, err := os.Create(csvFilePath)
	if err != nil {
		return "", nil, fmt.Errorf("creating CSV file: %w", err)
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
		return "", nil, fmt.Errorf("writing CSV header: %w", err)
	}

	headerIndex := make(map[string]int, len(headers))
	for i, key := range headers {
		headerIndex[key] = i
	}

	externalIDIndex, ok := headerIndex[externalIDField]
	if !ok {
		return "", nil, fmt.Errorf("externalId field %q not present in job data", externalIDField)
	}

	externalIDToJobID := make(map[string][]int64)
	for _, job := range input {
		row := make([]string, len(headers))
		for key, value := range job.Message {
			if idx, ok := headerIndex[key]; ok {
				formattedValue, err := common.FormatCSVValue(value)
				if err != nil {
					return "", nil, fmt.Errorf("formatting CSV cell %q: %w", key, err)
				}
				row[idx] = formattedValue
			}
		}
		jobID := int64(job.Metadata["job_id"].(float64))
		if err := writer.Write(row); err != nil {
			return "", nil, fmt.Errorf("writing CSV row: %w", err)
		}
		// Correlate on the externalId value alone, not the whole row: Salesforce
		// coerces other columns on store (datetime ms-truncation, number scale,
		// ...) so a whole-row key would not survive the round-trip. Events with
		// an empty externalId are dropped in Upload before reaching here.
		key := row[externalIDIndex]
		externalIDToJobID[key] = append(externalIDToJobID[key], jobID)
	}

	return csvFilePath, externalIDToJobID, nil
}

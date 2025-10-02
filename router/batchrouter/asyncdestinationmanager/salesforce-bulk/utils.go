package salesforcebulk

import (
	"bufio"
	"crypto/sha256"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

const (
	maxFileSize = 100 * 1024 * 1024 // 100MB - Salesforce Bulk API 2.0 limit
)

// ObjectInfo contains object type and external ID field extracted from VDM context
type ObjectInfo struct {
	ObjectType      string
	ExternalIDField string
}

// readJobsFromFile reads and unmarshals jobs from the file created by batch router
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

// extractObjectInfo extracts Salesforce object type and external ID
// For RETL/VDM: Reads from context.externalId (provided by VDM)
// For event streams: Uses config.ObjectType (defaults to Lead)
func extractObjectInfo(jobs []common.AsyncJob, config DestinationConfig) (*ObjectInfo, error) {
	if len(jobs) == 0 {
		return nil, fmt.Errorf("no jobs to process")
	}

	firstJob := jobs[0]

	// Try VDM/RETL path first (has externalId from VDM)
	if externalIDRaw, ok := firstJob.Metadata["externalId"]; ok {
		return extractFromVDM(externalIDRaw)
	}

	// Event stream path - use config
	objectType := config.ObjectType
	if objectType == "" {
		objectType = "Lead" // Salesforce default for event streams
	}

	return &ObjectInfo{
		ObjectType:      objectType,
		ExternalIDField: "Email", // Default for upsert operations
	}, nil
}

// extractFromVDM extracts object info from VDM's externalId structure
func extractFromVDM(externalIDRaw interface{}) (*ObjectInfo, error) {

	// externalId is an array, get first element
	externalIDArray, ok := externalIDRaw.([]interface{})
	if !ok || len(externalIDArray) == 0 {
		return nil, fmt.Errorf("externalId must be an array with at least one element")
	}

	externalIDMap, ok := externalIDArray[0].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("externalId[0] must be an object")
	}

	// Extract type (e.g., "Salesforce-Contact")
	typeStr, _ := externalIDMap["type"].(string)
	if typeStr == "" {
		return nil, fmt.Errorf("externalId type is required")
	}

	// Remove "Salesforce-" prefix to get object type
	objectType := strings.Replace(typeStr, "Salesforce-", "", 1)
	objectType = strings.Replace(objectType, "salesforce-", "", 1)

	// Extract identifierType (e.g., "Email") - used as external ID field for upsert
	identifierType, _ := externalIDMap["identifierType"].(string)

	return &ObjectInfo{
		ObjectType:      objectType,
		ExternalIDField: identifierType,
	}, nil
}

// createCSVFile generates a CSV file from transformed Salesforce payloads
func createCSVFile(
	destinationID string,
	input []common.AsyncJob,
	dataHashToJobID map[string]int64,
) (string, []int64, []int64, error) {
	csvFilePath := fmt.Sprintf("/tmp/salesforce_%s_%d.csv", destinationID, time.Now().Unix())
	csvFile, err := os.Create(csvFilePath)
	if err != nil {
		return "", nil, nil, fmt.Errorf("creating CSV file: %w", err)
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	var insertedJobIDs []int64
	var overflowedJobIDs []int64

	// Collect all field names from first job to build headers
	if len(input) == 0 {
		return "", nil, nil, fmt.Errorf("no jobs to process")
	}

	// Build headers from first job's message keys
	var headers []string
	headerIndex := make(map[string]int)
	for key := range input[0].Message {
		headerIndex[key] = len(headers)
		headers = append(headers, key)
	}

	// Write header row
	if err := writer.Write(headers); err != nil {
		return "", nil, nil, fmt.Errorf("writing CSV header: %w", err)
	}

	currentSize := int64(len(strings.Join(headers, ",")) + 1) // Header size

	// Write data rows
	for _, job := range input {
		row := make([]string, len(headers))

		// Fill row with values from message
		for key, value := range job.Message {
			if idx, ok := headerIndex[key]; ok {
				row[idx] = fmt.Sprintf("%v", value)
			}
		}

		// Calculate row size
		rowSize := int64(len(strings.Join(row, ",")) + 1)

		jobID := int64(job.Metadata["job_id"].(float64))

		// Check file size limit
		if currentSize+rowSize > maxFileSize {
			overflowedJobIDs = append(overflowedJobIDs, jobID)
			continue
		}

		// Write row
		if err := writer.Write(row); err != nil {
			return "", nil, nil, fmt.Errorf("writing CSV row: %w", err)
		}

		currentSize += rowSize
		insertedJobIDs = append(insertedJobIDs, jobID)

		// Track hash for result matching
		hash := calculateHashCode(row)
		dataHashToJobID[hash] = jobID
	}

	return csvFilePath, insertedJobIDs, overflowedJobIDs, nil
}

// calculateHashCode generates a hash from CSV row for tracking
func calculateHashCode(row []string) string {
	joined := strings.Join(row, ",")
	hash := sha256.Sum256([]byte(joined))
	return fmt.Sprintf("%x", hash)
}

// calculateHashFromRecord generates hash from Salesforce result record
func calculateHashFromRecord(record map[string]string) string {
	// Salesforce returns records with same fields as CSV
	// Build same row structure for matching
	var values []string
	for _, value := range record {
		values = append(values, value)
	}
	return calculateHashCode(values)
}


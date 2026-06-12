package salesforcebulkupload

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

// countingWriter wraps an io.Writer and tracks the number of bytes written
// through it, so createCSVFile can report the CSV size without a second stat.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

func extractObjectInfo(jobs []SalesforceAsyncJob) (*ObjectInfo, error) {
	if len(jobs) == 0 {
		return nil, fmt.Errorf("no jobs to process")
	}
	externalID := jobs[0].Metadata.ExternalID
	if len(externalID) == 0 {
		return nil, fmt.Errorf("externalId not found in the first job")
	}

	objectInfo, err := extractFromVDM(externalID)
	if err != nil {
		return nil, fmt.Errorf("failed to extract object info: %w", err)
	}
	return objectInfo, nil
}

func extractFromVDM(externalIDs []SalesforceExternalID) (*ObjectInfo, error) {
	if len(externalIDs) == 0 {
		return nil, fmt.Errorf("externalId must be an array with at least one element")
	}

	externalID := externalIDs[0]
	if externalID.Type == "" {
		return nil, fmt.Errorf("externalId type is required")
	}
	if externalID.IdentifierType == "" {
		return nil, fmt.Errorf("externalId identifierType is required")
	}
	return &ObjectInfo{
		ObjectType:      strings.Replace(externalID.Type, "SALESFORCE_BULK_UPLOAD-", "", 1),
		ExternalIDField: externalID.IdentifierType,
		ExternalIDValue: externalID.ID,
	}, nil
}

func createCSVFile(
	destinationID string,
	externalIDField string,
	input []SalesforceAsyncJob,
) (string, map[string][]int64, int64, error) {
	if err := os.MkdirAll(CSVDir, 0o755); err != nil {
		return "", nil, 0, fmt.Errorf("creating CSV directory: %w", err)
	}
	csvFilePath := filepath.Join(CSVDir, fmt.Sprintf("salesforce_%s_%d.csv", destinationID, time.Now().Unix()))
	csvFile, err := os.Create(csvFilePath)
	if err != nil {
		return "", nil, 0, fmt.Errorf("creating CSV file: %w", err)
	}
	defer csvFile.Close()

	counter := &countingWriter{w: csvFile}
	writer := csv.NewWriter(counter)

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
		return "", nil, 0, fmt.Errorf("writing CSV header: %w", err)
	}

	headerIndex := make(map[string]int, len(headers))
	for i, key := range headers {
		headerIndex[key] = i
	}

	externalIDIndex, ok := headerIndex[externalIDField]
	if !ok {
		return "", nil, 0, fmt.Errorf("externalId field %q not present in job data", externalIDField)
	}

	externalIDToJobID := make(map[string][]int64)
	for _, job := range input {
		row := make([]string, len(headers))
		for key, value := range job.Message {
			if idx, ok := headerIndex[key]; ok {
				formattedValue, err := common.FormatCSVValue(value)
				if err != nil {
					return "", nil, 0, fmt.Errorf("formatting CSV cell %q: %w", key, err)
				}
				row[idx] = formattedValue
			}
		}
		jobID := job.Metadata.JobID
		if err := writer.Write(row); err != nil {
			return "", nil, 0, fmt.Errorf("writing CSV row: %w", err)
		}
		// Correlate on the externalId value alone, not the whole row: Salesforce
		// coerces other columns on store (datetime ms-truncation, number scale,
		// ...) so a whole-row key would not survive the round-trip. Events with
		// an empty externalId are dropped in Upload before reaching here.
		key := row[externalIDIndex]
		externalIDToJobID[key] = append(externalIDToJobID[key], jobID)
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", nil, 0, fmt.Errorf("flushing CSV writer: %w", err)
	}

	return csvFilePath, externalIDToJobID, counter.n, nil
}

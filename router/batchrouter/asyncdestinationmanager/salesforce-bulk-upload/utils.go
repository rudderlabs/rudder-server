package salesforcebulkupload

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
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

// hashExternalID returns the SHA-256 hex digest of an externalId value. The
// externalId is often PII (e.g. an email), so we persist only its hash in the
// importing job status params and correlate by re-hashing the Salesforce-returned
// externalId at poll time. This is safe because Salesforce returns the upsert key
// unchanged (its value coercion only affects non-key columns).
func hashExternalID(externalID string) string {
	sum := sha256.Sum256([]byte(externalID))
	return hex.EncodeToString(sum[:])
}

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
) (string, int64, error) {
	if err := os.MkdirAll(CSVDir, 0o755); err != nil {
		return "", 0, fmt.Errorf("creating CSV directory: %w", err)
	}
	csvFilePath := filepath.Join(CSVDir, fmt.Sprintf("salesforce_%s_%d.csv", destinationID, time.Now().Unix()))
	csvFile, err := os.Create(csvFilePath)
	if err != nil {
		return "", 0, fmt.Errorf("creating CSV file: %w", err)
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
		return "", 0, fmt.Errorf("writing CSV header: %w", err)
	}

	headerIndex := make(map[string]int, len(headers))
	for i, key := range headers {
		headerIndex[key] = i
	}

	// Correlation no longer relies on this file — the externalId → jobID mapping
	// is rebuilt at poll time from the importing job status params. We only
	// validate the upsert key column is present so Salesforce can match on it.
	if _, ok := headerIndex[externalIDField]; !ok {
		return "", 0, fmt.Errorf("externalId field %q not present in job data", externalIDField)
	}

	for _, job := range input {
		row := make([]string, len(headers))
		for key, value := range job.Message {
			if idx, ok := headerIndex[key]; ok {
				formattedValue, err := common.FormatCSVValue(value)
				if err != nil {
					return "", 0, fmt.Errorf("formatting CSV cell %q: %w", key, err)
				}
				row[idx] = formattedValue
			}
		}
		if err := writer.Write(row); err != nil {
			return "", 0, fmt.Errorf("writing CSV row: %w", err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", 0, fmt.Errorf("flushing CSV writer: %w", err)
	}

	return csvFilePath, counter.n, nil
}

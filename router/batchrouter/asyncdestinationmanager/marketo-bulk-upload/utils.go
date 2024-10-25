package marketobulkupload

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

const (
	maxFileSize    = 10 * 1024 * 1024 // 10MB in bytes
	estimateBuffer = 0.95             // 95% of max size to account for any calculation discrepancies
	fileName       = "marketo_bulk_upload"
)

// Docs: https://experienceleague.adobe.com/en/docs/marketo-developer/marketo/rest/error-codes
func categorizeMarketoError(errorCode string) (status, message string) {
	// Convert string error code to integer
	code, err := strconv.Atoi(errorCode)
	if err != nil {
		return "Retryable", "Unknown"
	}

	switch {

	// Specific retryable errors mentioned in the document
	case code == 502:
		return "Retryable", "Bad Gateway"
	case code == 604:
		return "Retryable", "Request time-out"
	case code == 608:
		return "Retryable", "API Temporarily Unavailable"
	case code == 611:
		return "Retryable", "System error"
	case code == 614:
		return "Retryable", "Invalid Subscription"
	case code == 713:
		return "Retryable", "Transient Error"
	case code == 719:
		return "Retryable", "Lock wait timeout exception"

	// 5XX errors are generally retryable
	case code >= 500 && code < 600:
		return "Retryable", "Internal Server Error"

	// Rate limiting and quota errors
	case code == 606:
		return "Throttle", "Max rate limit exceeded"
	case code == 607:
		return "Throttle", "Daily quota reached"
	case code == 615:
		return "Throttle", "Concurrent access limit reached"
	case code == 1029:
		return "Throttle", "Too many jobs in queue or Export daily quota exceeded"

	// 4XX errors are generally not retryable (abortable)
	case code >= 400 && code < 500:
		return "Abortable", "Unknown"

	case code == 601:
		return "RefreshToken", "Access token invalid"
	case code == 602:
		return "RefreshToken", "Access token expired"

	// Specific abortable errors
	case code == 603:
		return "Abortable", "Access denied"
	case code == 605:
		return "Abortable", "HTTP Method not supported"
	case code == 609:
		return "Abortable", "Invalid JSON"
	case code == 610:
		return "Abortable", "Requested resource not found"
	case code == 612:
		return "Abortable", "Invalid Content Type"
	case code == 616:
		return "Abortable", "Invalid subscription type"

	// Add more specific error codes as needed

	default:
		return "Retryable", "Unknown"
	}
}

func handleMarketoErrorCode(errorCode string) (int, string, string) {
	category, message := categorizeMarketoError(errorCode)
	switch category {
	case "Retryable":
		return 500, category, message
	case "Throttle":
		return 429, category, message
	case "Abortable":
		return 400, category, message
	case "RefreshToken":
		return 500, category, message
	default:
		return 500, category, message
	}
}

func parseMarketoResponse(marketoResponse MarketoResponse) (int, string, string) {
	statusCode := 200
	category := "Success"
	errorMessage := ""
	if !marketoResponse.Success {
		if len(marketoResponse.Errors) > 0 {
			errorCode := marketoResponse.Errors[0].Code
			message := marketoResponse.Errors[0].Message

			statusCode, category, errorMessage = handleMarketoErrorCode(errorCode)
			errorMessage = fmt.Sprintf("%s: %s", message, errorMessage)
		} else {
			// Handle the case where Errors array is empty
			statusCode, category, errorMessage = handleMarketoErrorCode("Unknown")
		}
	}
	return statusCode, category, errorMessage
}

// ==== Response Parsing End ====

// calculateRowSize calculates the exact size of a CSV row after escaping
func calculateRowSize(row []string) (int64, error) {
	buf := &bytes.Buffer{}
	writer := csv.NewWriter(buf)
	writer.UseCRLF = true // Use CRLF as that's what encoding/csv uses

	err := writer.Write(row)
	if err != nil {
		return 0, fmt.Errorf("error writing to buffer: %w", err)
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return 0, fmt.Errorf("error flushing buffer: %w", err)
	}

	return int64(buf.Len()), nil
}

func createCSVFile(destinationID string, destConfig MarketoConfig, input []common.AsyncJob, dataHashToJobId map[string]int64) (string, []string, []int64, []int64, error) {
	csvFilePath := fmt.Sprintf("/tmp/%s_%s.csv", destinationID, fileName)
	csvFile, err := os.Create(csvFilePath)
	if err != nil {
		return "", nil, nil, nil, err
	}
	defer csvFile.Close()

	overflowedJobIDs := make([]int64, 0)
	insertedJobIDs := make([]int64, 0)

	headers := make(map[string]int)
	var headerOrder []string

	// Create a CSV writer
	writer := csv.NewWriter(csvFile)
	writer.UseCRLF = true // Use CRLF as that's what encoding/csv uses
	defer writer.Flush()

	// First pass: collect all unique headers we are taking value as its the marketo field name
	for _, value := range destConfig.FieldsMapping {
		if _, exists := headers[value]; !exists {
			headers[value] = len(headerOrder)
			headerOrder = append(headerOrder, value)
		}
	}

	// Calculate and verify header size
	headerSize, err := calculateRowSize(headerOrder)
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("error calculating header size: %w", err)
	}

	if headerSize > maxFileSize {
		return "", nil, nil, nil, fmt.Errorf("header size exceeds maximum file size")
	}

	if err = writer.Write(headerOrder); err != nil {
		return "", nil, nil, nil, err
	}

	currentSize := headerSize
	maxSizeWithBuffer := int64(float64(maxFileSize) * estimateBuffer)

	// Second pass: write data rows
	for _, job := range input {
		message := job.Message
		row := make([]string, len(headerOrder))
		for key, value := range message {
			if _, exists := headers[key]; exists {
				row[headers[key]] = fmt.Sprintf("%v", value)
			}
		}

		// Calculate row size before writing
		rowSize, err := calculateRowSize(row)
		if err != nil {
			return "", nil, nil, nil, fmt.Errorf("error calculating row size: %w", err)
		}

		jobID := int64(job.Metadata["job_id"].(float64))

		// Check if adding this row would exceed the size limit
		if currentSize+rowSize > maxSizeWithBuffer {
			overflowedJobIDs = append(overflowedJobIDs, jobID)
			continue
		}

		// Write the row if it fits
		if err := writer.Write(row); err != nil {
			return "", nil, nil, nil, fmt.Errorf("error writing row: %w", err)
		}

		currentSize += rowSize

		insertedJobIDs = append(insertedJobIDs, jobID)
		// Calculate hash code for the row
		dataHash := calculateHashCode(row)
		// Store the mapping of data hash to job ID
		dataHashToJobId[dataHash] = jobID
	}

	return csvFilePath, headerOrder, insertedJobIDs, overflowedJobIDs, nil
}

func calculateHashCode(data []string) string {
	// Join the strings into a single string with a separator
	joinedStrings := strings.Join(data, ",")
	hash := sha256.New()
	hash.Write([]byte(joinedStrings))
	hashBytes := hash.Sum(nil)
	hashCode := fmt.Sprintf("%x", hashBytes)

	return hashCode
}

func sendHTTPRequest(uploadURL, csvFilePath, accessToken, deduplicationField string) (*http.Response, error) {
	file, err := os.Open(csvFilePath)
	if err != nil {
		return nil, fmt.Errorf("error while opening the file: %v", err)
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", filepath.Base(csvFilePath))
	if err != nil {
		return nil, fmt.Errorf("error while creating form file: %v", err)
	}
	_, err = io.Copy(part, file)
	if err != nil {
		return nil, fmt.Errorf("error while copying file: %v", err)
	}

	_ = writer.WriteField("format", "csv")
	_ = writer.WriteField("access_token", accessToken)
	if deduplicationField != "" {
		_ = writer.WriteField("lookupField", deduplicationField)
	}

	err = writer.Close()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", uploadURL, body)
	if err != nil {
		return nil, fmt.Errorf("error while creating request: %v", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	client := &http.Client{}
	return client.Do(req)
}

// ==== Upload Utils End ====

func readJobsFromFile(filePath string) ([]common.AsyncJob, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("BRT: Read File Failed: %v", err)
	}
	defer file.Close()

	input := make([]common.AsyncJob, 0)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var tempJob common.AsyncJob
		jobBytes := scanner.Bytes()
		err := jsonfast.Unmarshal(jobBytes, &tempJob)
		if err != nil {
			return nil, fmt.Errorf("BRT: Error in Unmarshalling Job: %v", err)
		}
		input = append(input, tempJob)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("BRT: Error in Scanning File: %v", err)
	}

	return input, nil
}

// ==== File Utils End ====

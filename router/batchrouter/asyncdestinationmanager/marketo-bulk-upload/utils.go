package marketobulkupload

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
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

// Docs: https://experienceleague.adobe.com/en/docs/marketo-developer/marketo/rest/error-codes
func categorizeMarketoError(errorCode string) (status string, message string) {
	// Convert string error code to integer
	code, err := strconv.Atoi(errorCode)
	if err != nil {
		return "Retryable", "Unknown"
	}

	switch {
	// 5XX errors are generally retryable
	case code >= 500 && code < 600:
		return "Retryable", "Internal Server Error"

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

func handleMarketoErrorCode(errorCode string) (int64, string, string) {
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

func parseMarketoResponse(marketoResponse MarketoResponse) (int64, string, string) {
	statusCode := int64(200)
	category := "Success"
	errorMessage := ""
	if !marketoResponse.Success {
		if len(marketoResponse.Errors) > 0 {
			errorCode := marketoResponse.Errors[0].Code
			errorMessage = marketoResponse.Errors[0].Message

			statusCode, category, _ = handleMarketoErrorCode(errorCode)
		} else {
			// Handle the case where Errors array is empty
			errorMessage = "Unknown error"
			statusCode, category, _ = handleMarketoErrorCode("Unknown")
		}

	}

	return statusCode, category, errorMessage
}

// ==== Response Parsing End ====

func createCSVFile(destinationID string, destConfig MarketoConfig, input []common.AsyncJob, dataHashToJobId map[string]int64) (string, []string, []int64, []int64, error) {
	csvFilePath := fmt.Sprintf("/tmp/%s_%s.csv", destinationID, "marketo_bulk_upload")
	csvFile, err := os.Create(csvFilePath)
	if err != nil {
		return "", nil, nil, nil, err
	}
	defer csvFile.Close()

	var overflowedJobIDs []int64
	var insertedJobIDs []int64
	var currentSize int64

	headers := make(map[string]int)
	var headerOrder []string

	// First pass: collect all unique headers we are taking value as its the marketo field name
	for _, value := range destConfig.FieldsMapping {
		if _, exists := headers[value]; !exists {
			headers[value] = len(headerOrder)
			headerOrder = append(headerOrder, value)
		}
	}

	// Write header row
	headerRow := strings.Join(headerOrder, ",") + "\n"
	_, err = csvFile.WriteString(headerRow)
	if err != nil {
		return "", nil, nil, nil, err
	}
	currentSize += int64(len(headerRow))

	// Second pass: write data rows
	for _, job := range input {
		message := job.Message
		row := make([]string, len(headerOrder))
		for key, value := range message {
			row[headers[key]] = fmt.Sprintf("%v", value)
		}
		line := strings.Join(row, ",") + "\n"
		lineSize := int64(len(line))

		if currentSize+lineSize > 10*1024*1024 { // 10MB in bytes
			overflowedJobIDs = append(overflowedJobIDs, int64(job.Metadata["job_id"].(float64)))
			continue
		}

		_, err := csvFile.WriteString(line)
		if err != nil {
			return "", nil, nil, nil, err
		}
		currentSize += lineSize
		insertedJobIDs = append(insertedJobIDs, int64(job.Metadata["job_id"].(float64)))
		// Calculate hash code for the row
		dataHash := calculateHashCode(row)
		// Store the mapping of data hash to job ID
		dataHashToJobId[dataHash] = int64(job.Metadata["job_id"].(float64))
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

func sendHTTPRequest(uploadURL, csvFilePath string, accessToken string, deduplicationField string) (*http.Response, error) {
	file, err := os.Open(csvFilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", filepath.Base(csvFilePath))

	if err != nil {
		return nil, err
	}
	_, err = io.Copy(part, file)
	if err != nil {
		return nil, err
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
		return nil, err
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

	var input []common.AsyncJob
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var tempJob common.AsyncJob
		jobBytes := scanner.Bytes()
		err := json.Unmarshal(jobBytes, &tempJob)
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

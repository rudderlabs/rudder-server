package salesforcebulk

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

// NewSalesforceAPIService creates a new Salesforce Bulk API 2.0 client
func NewSalesforceAPIService(
	authService SalesforceAuthServiceInterface,
	logger logger.Logger,
	apiVersion string,
) *SalesforceAPIService {
	return &SalesforceAPIService{
		authService: authService,
		logger:      logger,
		apiVersion:  apiVersion,
	}
}

// CreateJob creates a new Bulk API 2.0 ingest job
func (s *SalesforceAPIService) CreateJob(
	objectName, operation, externalIDField string,
) (string, *APIError) {
	reqBody := JobCreateRequest{
		Object:      objectName,
		ContentType: "CSV",
		Operation:   operation,
		LineEnding:  "LF",
	}

	// Add external ID field for upsert operations
	if operation == "upsert" && externalIDField != "" {
		reqBody.ExternalIDFieldName = externalIDField
	}

	body, err := jsonrs.Marshal(reqBody)
	if err != nil {
		return "", &APIError{
			StatusCode: 500,
			Message:    fmt.Sprintf("marshalling job request: %v", err),
			Category:   "ServerError",
		}
	}

	endpoint := fmt.Sprintf("%s/services/data/%s/jobs/ingest",
		s.authService.GetInstanceURL(), s.apiVersion)

	respBody, apiErr := s.makeRequest("POST", endpoint, bytes.NewReader(body), "application/json")
	if apiErr != nil {
		return "", apiErr
	}

	var jobResp JobResponse
	if err := jsonrs.Unmarshal(respBody, &jobResp); err != nil {
		return "", &APIError{
			StatusCode: 500,
			Message:    fmt.Sprintf("unmarshalling job response: %v", err),
			Category:   "ServerError",
		}
	}

	s.logger.Debugf("Created Salesforce Bulk job %s for object %s", jobResp.ID, objectName)

	return jobResp.ID, nil
}

// UploadData uploads CSV data to an existing Bulk API job
func (s *SalesforceAPIService) UploadData(jobID, csvFilePath string) *APIError {
	file, err := os.Open(csvFilePath)
	if err != nil {
		return &APIError{
			StatusCode: 500,
			Message:    fmt.Sprintf("opening CSV file: %v", err),
			Category:   "ServerError",
		}
	}
	defer file.Close()

	endpoint := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s/batches",
		s.authService.GetInstanceURL(), s.apiVersion, jobID)

	_, apiErr := s.makeRequest("PUT", endpoint, file, "text/csv")
	if apiErr != nil {
		return apiErr
	}

	s.logger.Debugf("Uploaded data to Salesforce Bulk job %s", jobID)

	return nil
}

// CloseJob closes a Bulk API job to trigger processing
func (s *SalesforceAPIService) CloseJob(jobID string) *APIError {
	reqBody := map[string]string{"state": "UploadComplete"}
	body, _ := jsonrs.Marshal(reqBody)

	endpoint := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s",
		s.authService.GetInstanceURL(), s.apiVersion, jobID)

	_, apiErr := s.makeRequest("PATCH", endpoint, bytes.NewReader(body), "application/json")
	if apiErr != nil {
		return apiErr
	}

	s.logger.Debugf("Closed Salesforce Bulk job %s", jobID)

	return nil
}

// GetJobStatus retrieves the status of a Bulk API job
func (s *SalesforceAPIService) GetJobStatus(jobID string) (*JobResponse, *APIError) {
	endpoint := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s",
		s.authService.GetInstanceURL(), s.apiVersion, jobID)

	respBody, apiErr := s.makeRequest("GET", endpoint, nil, "")
	if apiErr != nil {
		return nil, apiErr
	}

	var jobResp JobResponse
	if err := jsonrs.Unmarshal(respBody, &jobResp); err != nil {
		return nil, &APIError{
			StatusCode: 500,
			Message:    fmt.Sprintf("unmarshalling job status: %v", err),
			Category:   "ServerError",
		}
	}

	return &jobResp, nil
}

// GetFailedRecords retrieves failed records from a completed job
func (s *SalesforceAPIService) GetFailedRecords(jobID string) ([]map[string]string, *APIError) {
	endpoint := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s/failedResults",
		s.authService.GetInstanceURL(), s.apiVersion, jobID)

	return s.getCSVRecords(endpoint)
}

// GetSuccessfulRecords retrieves successful records from a completed job
func (s *SalesforceAPIService) GetSuccessfulRecords(jobID string) ([]map[string]string, *APIError) {
	endpoint := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s/successfulResults",
		s.authService.GetInstanceURL(), s.apiVersion, jobID)

	return s.getCSVRecords(endpoint)
}

// DeleteJob deletes a Bulk API job
func (s *SalesforceAPIService) DeleteJob(jobID string) *APIError {
	endpoint := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s",
		s.authService.GetInstanceURL(), s.apiVersion, jobID)

	_, apiErr := s.makeRequest("DELETE", endpoint, nil, "")
	return apiErr
}

// getCSVRecords fetches and parses CSV results from Salesforce
func (s *SalesforceAPIService) getCSVRecords(endpoint string) ([]map[string]string, *APIError) {
	respBody, apiErr := s.makeRequest("GET", endpoint, nil, "")
	if apiErr != nil {
		return nil, apiErr
	}

	reader := csv.NewReader(bytes.NewReader(respBody))

	// Read header row
	headers, err := reader.Read()
	if err == io.EOF {
		// Empty results
		return []map[string]string{}, nil
	}
	if err != nil {
		return nil, &APIError{
			StatusCode: 500,
			Message:    fmt.Sprintf("reading CSV header: %v", err),
			Category:   "ServerError",
		}
	}

	// Read all records
	var records []map[string]string
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, &APIError{
				StatusCode: 500,
				Message:    fmt.Sprintf("reading CSV row: %v", err),
				Category:   "ServerError",
			}
		}

		record := make(map[string]string)
		for i, value := range row {
			if i < len(headers) {
				record[headers[i]] = value
			}
		}
		records = append(records, record)
	}

	return records, nil
}

// makeRequest makes an HTTP request to Salesforce with OAuth authentication
func (s *SalesforceAPIService) makeRequest(
	method, endpoint string,
	body io.Reader,
	contentType string,
) ([]byte, *APIError) {
	// Get OAuth access token
	token, err := s.authService.GetAccessToken()
	if err != nil {
		return nil, &APIError{
			StatusCode: 500,
			Message:    fmt.Sprintf("getting access token: %v", err),
			Category:   "RefreshToken",
		}
	}

	// Create HTTP request
	req, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		return nil, &APIError{
			StatusCode: 500,
			Message:    fmt.Sprintf("creating HTTP request: %v", err),
			Category:   "ServerError",
		}
	}

	// Set headers
	req.Header.Set("Authorization", "Bearer "+token)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	// Make request
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, &APIError{
			StatusCode: 500,
			Message:    fmt.Sprintf("making HTTP request: %v", err),
			Category:   "ServerError",
		}
	}
	defer resp.Body.Close()

	// Read response body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &APIError{
			StatusCode: 500,
			Message:    fmt.Sprintf("reading response body: %v", err),
			Category:   "ServerError",
		}
	}

	// Handle error status codes
	if resp.StatusCode >= 400 {
		return nil, &APIError{
			StatusCode: resp.StatusCode,
			Message:    string(respBody),
			Category:   categorizeError(resp.StatusCode, respBody),
		}
	}

	return respBody, nil
}

// categorizeError categorizes API errors for proper retry/abort handling
func categorizeError(statusCode int, body []byte) string {
	switch statusCode {
	case 401:
		return "RefreshToken"
	case 429:
		return "RateLimit"
	case 400:
		return "BadRequest"
	case 500, 502, 503, 504:
		return "ServerError"
	default:
		return "Unknown"
	}
}


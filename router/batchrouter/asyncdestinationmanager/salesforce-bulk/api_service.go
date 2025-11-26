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

func (s *SalesforceAPIService) CreateJob(
	objectName, operation, externalIDField string,
) (string, *APIError) {
	reqBody := JobCreateRequest{
		Object:      objectName,
		ContentType: "CSV",
		Operation:   operation,
		LineEnding:  "LF",
	}

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

	instanceURL, err := s.authService.GetInstanceURL()
	if err != nil {
		return "", &APIError{
			StatusCode: 500,
			Message:    fmt.Sprintf("getting instance URL: %v", err),
			Category:   "RefreshToken",
		}
	}

	endpoint := fmt.Sprintf("%s/services/data/%s/jobs/ingest", instanceURL, s.apiVersion)

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

	instanceURL, err := s.authService.GetInstanceURL()
	if err != nil {
		return &APIError{
			StatusCode: 500,
			Message:    fmt.Sprintf("getting instance URL: %v", err),
			Category:   "RefreshToken",
		}
	}

	endpoint := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s/batches", instanceURL, s.apiVersion, jobID)

	_, apiErr := s.makeRequest("PUT", endpoint, file, "text/csv")
	if apiErr != nil {
		return apiErr
	}

	s.logger.Debugf("Uploaded data to Salesforce Bulk job %s", jobID)

	return nil
}

func (s *SalesforceAPIService) CloseJob(jobID string) *APIError {
	reqBody := map[string]string{"state": "UploadComplete"}
	body, _ := jsonrs.Marshal(reqBody)

	instanceURL, err := s.authService.GetInstanceURL()
	if err != nil {
		return &APIError{
			StatusCode: 500,
			Message:    fmt.Sprintf("getting instance URL: %v", err),
			Category:   "RefreshToken",
		}
	}

	endpoint := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s", instanceURL, s.apiVersion, jobID)

	_, apiErr := s.makeRequest("PATCH", endpoint, bytes.NewReader(body), "application/json")
	if apiErr != nil {
		return apiErr
	}

	s.logger.Debugf("Closed Salesforce Bulk job %s", jobID)

	return nil
}

func (s *SalesforceAPIService) GetJobStatus(jobID string) (*JobResponse, *APIError) {
	instanceURL, err := s.authService.GetInstanceURL()
	if err != nil {
		return nil, &APIError{
			StatusCode: 500,
			Message:    fmt.Sprintf("getting instance URL: %v", err),
			Category:   "RefreshToken",
		}
	}

	endpoint := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s", instanceURL, s.apiVersion, jobID)

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

func (s *SalesforceAPIService) GetFailedRecords(jobID string) ([]map[string]string, *APIError) {
	instanceURL, err := s.authService.GetInstanceURL()
	if err != nil {
		return nil, &APIError{
			StatusCode: 500,
			Message:    fmt.Sprintf("getting instance URL: %v", err),
			Category:   "RefreshToken",
		}
	}

	endpoint := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s/failedResults", instanceURL, s.apiVersion, jobID)
	return s.getCSVRecords(endpoint)
}

func (s *SalesforceAPIService) GetSuccessfulRecords(jobID string) ([]map[string]string, *APIError) {
	instanceURL, err := s.authService.GetInstanceURL()
	if err != nil {
		return nil, &APIError{
			StatusCode: 500,
			Message:    fmt.Sprintf("getting instance URL: %v", err),
			Category:   "RefreshToken",
		}
	}

	endpoint := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s/successfulResults", instanceURL, s.apiVersion, jobID)
	return s.getCSVRecords(endpoint)
}

func (s *SalesforceAPIService) DeleteJob(jobID string) *APIError {
	instanceURL, err := s.authService.GetInstanceURL()
	if err != nil {
		return &APIError{
			StatusCode: 500,
			Message:    fmt.Sprintf("getting instance URL: %v", err),
			Category:   "RefreshToken",
		}
	}

	endpoint := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s", instanceURL, s.apiVersion, jobID)

	_, apiErr := s.makeRequest("DELETE", endpoint, nil, "")
	return apiErr
}

func (s *SalesforceAPIService) getCSVRecords(endpoint string) ([]map[string]string, *APIError) {
	respBody, apiErr := s.makeRequest("GET", endpoint, nil, "")
	if apiErr != nil {
		return nil, apiErr
	}

	reader := csv.NewReader(bytes.NewReader(respBody))

	headers, err := reader.Read()
	if err == io.EOF {
		return []map[string]string{}, nil
	}
	if err != nil {
		return nil, &APIError{
			StatusCode: 500,
			Message:    fmt.Sprintf("reading CSV header: %v", err),
			Category:   "ServerError",
		}
	}

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

func (s *SalesforceAPIService) makeRequest(
	method, endpoint string,
	body io.Reader,
	contentType string,
) ([]byte, *APIError) {
	// Buffer small bodies (JSON) so we can retry them
	// Large bodies (CSV files) are passed as *os.File and should not be buffered
	var bodyBytes []byte
	var canRetry bool

	if body != nil {
		// Check if it's a file (don't buffer large files)
		if _, isFile := body.(*os.File); isFile {
			canRetry = false
		} else {
			// Buffer the body so we can retry
			var err error
			bodyBytes, err = io.ReadAll(body)
			if err != nil {
				return nil, &APIError{
					StatusCode: 500,
					Message:    fmt.Sprintf("reading request body: %v", err),
					Category:   "ServerError",
				}
			}
			canRetry = true
		}
	} else {
		canRetry = true
	}

	// Create body reader for initial attempt
	var bodyReader io.Reader
	if bodyBytes != nil {
		bodyReader = bytes.NewReader(bodyBytes)
	} else {
		bodyReader = body
	}

	// Initial attempt
	respBody, apiError := s.attemptRequest(method, endpoint, bodyReader, contentType)
	if apiError == nil {
		return respBody, nil
	}

	// Retry logic for token refresh errors
	const maxRetries = 2
	if canRetry && apiError.Category == "RefreshToken" {
		for retryCount := 0; retryCount < maxRetries; retryCount++ {
			s.logger.Infof("Retrying Salesforce API request after token error (attempt %d/%d)", retryCount+1, maxRetries)

			// Clear cached token to force refresh on next GetAccessToken call
			if authSvc, ok := s.authService.(*SalesforceAuthService); ok {
				authSvc.clearToken()
			}

			// Small backoff
			time.Sleep(time.Duration((retryCount+1)*2) * time.Second)

			// Recreate body reader from buffered bytes
			if bodyBytes != nil {
				bodyReader = bytes.NewReader(bodyBytes)
			}

			respBody, apiError = s.attemptRequest(method, endpoint, bodyReader, contentType)
			if apiError == nil {
				return respBody, nil
			}

			// If it's not a token error anymore, stop retrying
			if apiError.Category != "RefreshToken" {
				break
			}
		}
	}

	return nil, apiError
}

func (s *SalesforceAPIService) attemptRequest(
	method, endpoint string,
	body io.Reader,
	contentType string,
) ([]byte, *APIError) {
	token, err := s.authService.GetAccessToken()
	if err != nil {
		return nil, &APIError{
			StatusCode: 500,
			Message:    fmt.Sprintf("getting access token: %v", err),
			Category:   "RefreshToken",
		}
	}

	req, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		return nil, &APIError{
			StatusCode: 500,
			Message:    fmt.Sprintf("creating HTTP request: %v", err),
			Category:   "ServerError",
		}
	}

	req.Header.Set("Authorization", "Bearer "+token)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

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

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &APIError{
			StatusCode: 500,
			Message:    fmt.Sprintf("reading response body: %v", err),
			Category:   "ServerError",
		}
	}

	if resp.StatusCode >= 400 {
		return nil, &APIError{
			StatusCode: resp.StatusCode,
			Message:    string(respBody),
			Category:   categorizeError(resp.StatusCode, respBody),
		}
	}

	return respBody, nil
}

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

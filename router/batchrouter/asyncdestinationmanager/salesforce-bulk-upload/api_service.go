package salesforcebulkupload

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	cntx "github.com/rudderlabs/rudder-server/services/oauth/v2/context"
)

const csvRequestBodyLogCap = 2 * 1024

func newAPIService(
	logger logger.Logger,
	destination *backendconfig.DestinationT,
	client *http.Client,
	logRequestsDestIDs config.ValueLoader[[]string],
) APIServiceInterface {
	return &apiService{
		logger:             logger,
		destination:        destination,
		client:             client,
		logRequestsDestIDs: logRequestsDestIDs,
	}
}

func (s *apiService) CreateJob(
	objectName, operation, externalIDField string,
) (string, *APIError) {
	reqBody := jobCreateRequest{
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
			StatusCode: http.StatusInternalServerError,
			Message:    fmt.Sprintf("marshalling job request: %v", err),
			Category:   "ServerError",
		}
	}

	endpoint := ApiBaseURL + "/jobs/ingest"

	respBody, apiErr := s.makeRequest("CreateJob", http.MethodPost, endpoint, bytes.NewReader(body), "application/json")
	if apiErr != nil {
		return "", apiErr
	}

	var jobResp JobResponse
	if err := jsonrs.Unmarshal(respBody, &jobResp); err != nil {
		return "", &APIError{
			StatusCode: http.StatusInternalServerError,
			Message:    fmt.Sprintf("unmarshalling job response: %v", err),
			Category:   "ServerError",
		}
	}

	s.logger.Debugn("Created Salesforce Bulk job object",
		logger.NewStringField("jobID", jobResp.ID),
		logger.NewStringField("objectName", objectName),
	)

	return jobResp.ID, nil
}

func (s *apiService) UploadData(jobID, csvFilePath string) *APIError {
	file, err := os.Open(csvFilePath)
	if err != nil {
		return &APIError{
			StatusCode: http.StatusInternalServerError,
			Message:    fmt.Sprintf("opening CSV file: %v", err),
			Category:   "ServerError",
		}
	}
	defer func() { _ = file.Close() }()

	endpoint := ApiBaseURL + "/jobs/ingest/" + jobID + "/batches"

	_, apiErr := s.makeRequest("UploadData", http.MethodPut, endpoint, file, "text/csv")
	if apiErr != nil {
		return apiErr
	}

	s.logger.Debugn("Uploaded data to Salesforce Bulk job",
		logger.NewStringField("jobID", jobID),
	)

	return nil
}

func (s *apiService) CloseJob(jobID string) *APIError {
	reqBody := map[string]string{"state": "UploadComplete"}
	body, _ := jsonrs.Marshal(reqBody)

	endpoint := ApiBaseURL + "/jobs/ingest/" + jobID

	_, apiErr := s.makeRequest("CloseJob", http.MethodPatch, endpoint, bytes.NewReader(body), "application/json")
	if apiErr != nil {
		return apiErr
	}

	s.logger.Debugn("Closed Salesforce Bulk job",
		logger.NewStringField("jobID", jobID),
	)

	return nil
}

func (s *apiService) GetJobStatus(jobID string) (*JobResponse, *APIError) {
	endpoint := ApiBaseURL + "/jobs/ingest/" + jobID

	respBody, apiErr := s.makeRequest("GetJobStatus", http.MethodGet, endpoint, bytes.NewReader([]byte{}), "")
	if apiErr != nil {
		return nil, apiErr
	}

	var jobResp JobResponse
	if err := jsonrs.Unmarshal(respBody, &jobResp); err != nil {
		return nil, &APIError{
			StatusCode: http.StatusInternalServerError,
			Message:    fmt.Sprintf("unmarshalling job status: %v", err),
			Category:   "ServerError",
		}
	}

	return &jobResp, nil
}

func (s *apiService) GetFailedRecords(jobID string) ([]map[string]string, *APIError) {
	endpoint := ApiBaseURL + "/jobs/ingest/" + jobID + "/failedResults"
	return s.getCSVRecords(endpoint)
}

func (s *apiService) GetSuccessfulRecords(jobID string) ([]map[string]string, *APIError) {
	endpoint := ApiBaseURL + "/jobs/ingest/" + jobID + "/successfulResults"
	return s.getCSVRecords(endpoint)
}

func (s *apiService) DeleteJob(jobID string) *APIError {
	endpoint := ApiBaseURL + "/jobs/ingest/" + jobID

	_, apiErr := s.makeRequest("DeleteJob", http.MethodDelete, endpoint, bytes.NewReader([]byte{}), "")
	return apiErr
}

func (s *apiService) getCSVRecords(endpoint string) ([]map[string]string, *APIError) {
	respBody, apiErr := s.makeRequest("GetCSVRecords", http.MethodGet, endpoint, bytes.NewReader([]byte{}), "")
	if apiErr != nil {
		return nil, apiErr
	}

	reader := csv.NewReader(bytes.NewReader(respBody))

	headers, err := reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, &APIError{
			StatusCode: http.StatusInternalServerError,
			Message:    fmt.Sprintf("reading CSV header: %v", err),
			Category:   "ServerError",
		}
	}

	var records []map[string]string
	for {
		row, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, &APIError{
				StatusCode: http.StatusInternalServerError,
				Message:    fmt.Sprintf("reading CSV row: %v", err),
				Category:   "ServerError",
			}
		}

		record := make(map[string]string, len(headers))
		for i, value := range row {
			record[headers[i]] = value
		}
		records = append(records, record)
	}

	return records, nil
}

func (s *apiService) makeRequest(
	operation, method, endpoint string,
	body io.Reader,
	contentType string,
) ([]byte, *APIError) {
	logEnabled := s.shouldLogRequests()

	var requestBodyForLog []byte
	if logEnabled && body != nil {
		buffered, readErr := io.ReadAll(body)
		if readErr != nil {
			return nil, &APIError{
				StatusCode: http.StatusInternalServerError,
				Message:    fmt.Sprintf("reading request body for logging: %v", readErr),
				Category:   "ServerError",
			}
		}
		requestBodyForLog = buffered
		body = bytes.NewReader(buffered)
	}

	req, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		return nil, &APIError{
			StatusCode: http.StatusInternalServerError,
			Message:    fmt.Sprintf("creating HTTP request: %v", err),
			Category:   "ServerError",
		}
	}

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	req = req.WithContext(cntx.CtxWithDestination(req.Context(), s.destination))

	startTime := time.Now()
	resp, err := s.client.Do(req)
	latencyMs := time.Since(startTime).Milliseconds()
	if err != nil {
		if logEnabled {
			s.logger.Infon("Salesforce API request failed",
				logger.NewStringField("operation", operation),
				logger.NewStringField("method", method),
				logger.NewStringField("url", req.URL.String()),
				logger.NewStringField("contentType", contentType),
				logger.NewStringField("requestHeaders", formatHeaders(req.Header)),
				logger.NewStringField("requestBody", previewRequestBody(contentType, requestBodyForLog)),
				logger.NewIntField("requestBodyBytes", int64(len(requestBodyForLog))),
				logger.NewIntField("latencyMs", latencyMs),
				logger.NewStringField("transportError", err.Error()),
			)
		}
		return nil, &APIError{
			StatusCode: http.StatusInternalServerError,
			Message:    fmt.Sprintf("making HTTP request: %v", err),
			Category:   "ServerError",
		}
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &APIError{
			StatusCode: http.StatusInternalServerError,
			Message:    fmt.Sprintf("reading response body: %v", err),
			Category:   "ServerError",
		}
	}

	responseBodyForLog := respBody
	if unwrapped := gjson.GetBytes(respBody, "originalResponse").String(); unwrapped != "" {
		responseBodyForLog = []byte(unwrapped)
	}

	if logEnabled {
		s.logger.Infon("Salesforce API request/response",
			logger.NewStringField("operation", operation),
			logger.NewStringField("method", method),
			logger.NewStringField("url", req.URL.String()),
			logger.NewStringField("contentType", contentType),
			logger.NewStringField("requestHeaders", formatHeaders(req.Header)),
			logger.NewStringField("requestBody", previewRequestBody(contentType, requestBodyForLog)),
			logger.NewIntField("requestBodyBytes", int64(len(requestBodyForLog))),
			logger.NewIntField("responseStatusCode", int64(resp.StatusCode)),
			logger.NewStringField("responseHeaders", formatHeaders(resp.Header)),
			logger.NewStringField("responseBody", string(responseBodyForLog)),
			logger.NewIntField("responseBodyBytes", int64(len(responseBodyForLog))),
			logger.NewIntField("latencyMs", latencyMs),
		)
	}

	if resp.StatusCode >= http.StatusBadRequest {
		return nil, &APIError{
			StatusCode: resp.StatusCode,
			Message:    string(respBody),
			Category:   categorizeError(resp.StatusCode),
		}
	}

	if len(responseBodyForLog) > 0 && !bytes.Equal(responseBodyForLog, respBody) {
		respBody = responseBodyForLog
	}
	return respBody, nil
}

func (s *apiService) shouldLogRequests() bool {
	if s.logRequestsDestIDs == nil || s.destination == nil {
		return false
	}
	return slices.Contains(s.logRequestsDestIDs.Load(), s.destination.ID)
}

func formatHeaders(h http.Header) string {
	if len(h) == 0 {
		return ""
	}
	parts := make([]string, 0, len(h))
	for key, values := range h {
		redacted := values
		if strings.EqualFold(key, "Authorization") {
			redacted = []string{"Bearer [REDACTED]"}
		}
		parts = append(parts, fmt.Sprintf("%s: %s", key, strings.Join(redacted, ", ")))
	}
	slices.Sort(parts)
	return strings.Join(parts, "; ")
}

func previewRequestBody(contentType string, body []byte) string {
	if len(body) == 0 {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(contentType), "text/csv") && len(body) > csvRequestBodyLogCap {
		return fmt.Sprintf("%s...[truncated, total %d bytes]", string(body[:csvRequestBodyLogCap]), len(body))
	}
	return string(body)
}

func categorizeError(statusCode int) string {
	switch statusCode {
	case http.StatusUnauthorized:
		return "RefreshToken"
	case http.StatusTooManyRequests:
		return "RateLimit"
	case http.StatusBadRequest:
		return "BadRequest"
	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return "ServerError"
	default:
		return "Unknown"
	}
}

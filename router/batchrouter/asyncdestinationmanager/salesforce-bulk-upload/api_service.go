package salesforcebulkupload

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	cntx "github.com/rudderlabs/rudder-server/services/oauth/v2/context"
)

func newAPIService(
	logger logger.Logger,
	destinationInfo *oauthv2.DestinationInfo,
	client *http.Client,
) APIServiceInterface {
	return &apiService{
		logger:          logger,
		destinationInfo: destinationInfo,
		client:          client,
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

	respBody, apiErr := s.makeRequest(http.MethodPost, endpoint, bytes.NewReader(body), "application/json")
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

	_, apiErr := s.makeRequest(http.MethodPut, endpoint, file, "text/csv")
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

	_, apiErr := s.makeRequest(http.MethodPatch, endpoint, bytes.NewReader(body), "application/json")
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

	respBody, apiErr := s.makeRequest(http.MethodGet, endpoint, bytes.NewReader([]byte{}), "")
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

	_, apiErr := s.makeRequest(http.MethodDelete, endpoint, bytes.NewReader([]byte{}), "")
	return apiErr
}

func (s *apiService) getCSVRecords(endpoint string) ([]map[string]string, *APIError) {
	respBody, apiErr := s.makeRequest(http.MethodGet, endpoint, bytes.NewReader([]byte{}), "")
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
	method, endpoint string,
	body io.Reader,
	contentType string,
) ([]byte, *APIError) {
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

	req = req.WithContext(cntx.CtxWithDestInfo(req.Context(), s.destinationInfo))

	resp, err := s.client.Do(req)
	if err != nil {
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

	if resp.StatusCode >= http.StatusBadRequest {
		return nil, &APIError{
			StatusCode: resp.StatusCode,
			Message:    string(respBody),
			Category:   categorizeError(resp.StatusCode),
		}
	}

	originalResponse := gjson.GetBytes(respBody, "originalResponse").String()
	if originalResponse != "" {
		respBody = []byte(originalResponse)
	}
	return respBody, nil
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

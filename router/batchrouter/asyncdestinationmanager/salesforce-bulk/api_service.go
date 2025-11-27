package salesforcebulk

import (
	"bytes"
	"encoding/csv"
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

func NewSalesforceAPIService(
	logger logger.Logger,
	destinationInfo *oauthv2.DestinationInfo,
	apiVersion string,
	client *http.Client,
) *SalesforceAPIService {
	return &SalesforceAPIService{
		logger:          logger,
		destinationInfo: destinationInfo,
		apiVersion:      apiVersion,
		client:          client,
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

	endpoint := fmt.Sprintf("https://default.salesforce.com/services/data/%s/jobs/ingest", s.apiVersion)

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

	s.logger.Debugn("Created Salesforce Bulk job %s for object %s", logger.NewStringField("jobID", jobResp.ID), logger.NewStringField("objectName", objectName))

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

	endpoint := fmt.Sprintf("https://default.salesforce.com/services/data/%s/jobs/ingest/%s/batches", s.apiVersion, jobID)

	_, apiErr := s.makeRequest("PUT", endpoint, file, "text/csv")
	if apiErr != nil {
		return apiErr
	}

	s.logger.Debugn("Uploaded data to Salesforce Bulk job %s", logger.NewStringField("jobID", jobID))

	return nil
}

func (s *SalesforceAPIService) CloseJob(jobID string) *APIError {
	reqBody := map[string]string{"state": "UploadComplete"}
	body, _ := jsonrs.Marshal(reqBody)

	endpoint := fmt.Sprintf("https://default.salesforce.com/services/data/%s/jobs/ingest/%s", s.apiVersion, jobID)

	_, apiErr := s.makeRequest("PATCH", endpoint, bytes.NewReader(body), "application/json")
	if apiErr != nil {
		return apiErr
	}

	s.logger.Debugn("Closed Salesforce Bulk job %s", logger.NewStringField("jobID", jobID))

	return nil
}

func (s *SalesforceAPIService) GetJobStatus(jobID string) (*JobResponse, *APIError) {
	endpoint := fmt.Sprintf("https://default.salesforce.com/services/data/%s/jobs/ingest/%s", s.apiVersion, jobID)

	respBody, apiErr := s.makeRequest("GET", endpoint, bytes.NewReader([]byte{}), "")
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
	endpoint := fmt.Sprintf("https://default.salesforce.com/services/data/%s/jobs/ingest/%s/failedResults", s.apiVersion, jobID)
	return s.getCSVRecords(endpoint)
}

func (s *SalesforceAPIService) GetSuccessfulRecords(jobID string) ([]map[string]string, *APIError) {
	endpoint := fmt.Sprintf("https://default.salesforce.com/services/data/%s/jobs/ingest/%s/successfulResults", s.apiVersion, jobID)
	return s.getCSVRecords(endpoint)
}

func (s *SalesforceAPIService) DeleteJob(jobID string) *APIError {
	endpoint := fmt.Sprintf("https://default.salesforce.com/services/data/%s/jobs/ingest/%s", s.apiVersion, jobID)

	_, apiErr := s.makeRequest("DELETE", endpoint, bytes.NewReader([]byte{}), "")
	return apiErr
}

func (s *SalesforceAPIService) getCSVRecords(endpoint string) ([]map[string]string, *APIError) {
	respBody, apiErr := s.makeRequest("GET", endpoint, bytes.NewReader([]byte{}), "")
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
	respBody, apiError := s.attemptRequest(method, endpoint, body, contentType)
	if apiError == nil {
		return respBody, nil
	}

	return nil, apiError
}

func (s *SalesforceAPIService) attemptRequest(
	method, endpoint string,
	body io.Reader,
	contentType string,
) ([]byte, *APIError) {
	req, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		return nil, &APIError{
			StatusCode: 500,
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
			StatusCode: 500,
			Message:    fmt.Sprintf("making HTTP request: %v", err),
			Category:   "ServerError",
		}
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	fmt.Println("response", string(respBody))
	originalResponse := gjson.GetBytes(respBody, "originalResponse").String()
	if originalResponse != "" {
		respBody = []byte(originalResponse)
	}
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
			Category:   categorizeError(resp.StatusCode),
		}
	}

	return respBody, nil
}

func categorizeError(statusCode int) string {
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

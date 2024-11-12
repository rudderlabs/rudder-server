package marketobulkupload

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

type MarketoAPIServiceInterface interface {
	ImportLeads(csvFilePath, deduplicationField string) (string, *APIError)
	PollImportStatus(importId string) (*MarketoResponse, *APIError)
	GetLeadStatus(url string) ([]map[string]string, *APIError)
}

type MarketoAPIService struct {
	logger       logger.Logger
	statsFactory stats.Stats
	httpClient   *http.Client
	munchkinId   string
	authService  MarketoAuthServiceInterface
	maxRetries   int
}

type APIError struct {
	StatusCode int
	Category   string
	Message    string
}

func (m *MarketoAPIService) checkForCSVLikeResponse(resp *http.Response) bool {
	// check for csv like response by checking the headers
	respHeaders := resp.Header
	return respHeaders.Get("Content-Type") == "text/csv;charset=UTF-8"
}

func (m *MarketoAPIService) attemptImport(uploadURL, csvFilePath, deduplicationField string, uploadTimeStat stats.Measurement) (string, *APIError) {
	token, err := m.authService.GetAccessToken()
	if err != nil {
		return "", &APIError{StatusCode: 500, Category: "Retryable", Message: "Error in fetching access token"}
	}

	startTime := time.Now()
	resp, err := sendHTTPRequest(uploadURL, csvFilePath, token, deduplicationField)
	uploadTimeStat.Since(startTime)

	if err != nil {
		return "", &APIError{StatusCode: 500, Category: "Retryable", Message: "Error in sending request"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", &APIError{StatusCode: resp.StatusCode, Category: "Retryable", Message: "Error in sending request"}
	}

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", &APIError{StatusCode: 500, Category: "Retryable", Message: "Error in reading response body"}
	}

	var marketoResponse MarketoResponse
	err = json.Unmarshal(responseBody, &marketoResponse)
	if err != nil {
		return "", &APIError{StatusCode: 500, Category: "Retryable", Message: "Error in parsing response body"}
	}

	statusCode, category, errorMessage := parseMarketoResponse(marketoResponse)
	if category == "Success" {
		return marketoResponse.Result[0].ImportID, nil
	}

	return "", &APIError{StatusCode: statusCode, Category: category, Message: errorMessage}
}

func (m *MarketoAPIService) ImportLeads(csvFilePath, deduplicationField string) (string, *APIError) {
	uploadTimeStat := m.statsFactory.NewTaggedStat("async_upload_time", stats.TimerType, map[string]string{
		"module":   "batch_router",
		"destType": "MARKETO_BULK_UPLOAD",
	})

	uploadURL := fmt.Sprintf("https://%s.mktorest.com/bulk/v1/leads.json", m.munchkinId)

	// Initial attempt
	importID, apiError := m.attemptImport(uploadURL, csvFilePath, deduplicationField, uploadTimeStat)
	if apiError == nil {
		return importID, nil
	}

	retryCount := 0
	for retryCount < m.maxRetries {
		if apiError.Category == "RefreshToken" {
			time.Sleep(time.Duration((retryCount+1)*5) * time.Second)

			m.logger.Info(fmt.Sprintf("Retrying import after token expiry (attempt %d of %d)", retryCount+1, m.maxRetries))
			importID, apiError = m.attemptImport(uploadURL, csvFilePath, deduplicationField, uploadTimeStat)

			if apiError == nil {
				return importID, nil
			}

			retryCount++

		} else {
			// If it's not a token refresh error, don't retry
			return "", apiError
		}
	}

	return "", &APIError{
		StatusCode: 500,
		Category:   "Retryable",
		Message:    fmt.Sprintf("Failed to import after %d retries", m.maxRetries),
	}
}

func (m *MarketoAPIService) PollImportStatus(importId string) (*MarketoResponse, *APIError) {
	// poll for the import status

	apiURL := fmt.Sprintf("https://%s.mktorest.com/bulk/v1/leads/batch/%s.json", m.munchkinId, importId)
	token, err := m.authService.GetAccessToken()
	if err != nil {
		return nil, &APIError{StatusCode: 500, Category: "Retryable", Message: "Error in fetching access token"}
	}

	// Make the API request
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return nil, &APIError{StatusCode: 500, Category: "Retryable", Message: "Error in creating request"}
	}
	req.Header.Add("Authorization", "Bearer "+token)

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, &APIError{StatusCode: 500, Category: "Retryable", Message: "Error in sending request"}
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &APIError{StatusCode: 500, Category: "Retryable", Message: "Error in reading response body"}
	}

	var marketoResponse MarketoResponse

	err = json.Unmarshal(body, &marketoResponse)
	if err != nil {
		return nil, &APIError{StatusCode: 500, Category: "Retryable", Message: "Error in parsing response body"}
	}

	m.logger.Debugf("[Async Destination Manager] Marketo Poll Response: %v", marketoResponse)

	statusCode, category, errorMessage := parseMarketoResponse(marketoResponse)
	if category == "Success" {
		return &marketoResponse, nil
	}

	return nil, &APIError{StatusCode: statusCode, Category: category, Message: errorMessage}
}

func (m *MarketoAPIService) GetLeadStatus(url string) ([]map[string]string, *APIError) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, &APIError{StatusCode: 500, Category: "Retryable", Message: "Error in creating request"}
	}

	token, err := m.authService.GetAccessToken()
	if err != nil {
		return nil, &APIError{StatusCode: 500, Category: "Retryable", Message: "Error in fetching access token"}
	}

	req.Header.Add("Authorization", "Bearer "+token)

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, &APIError{StatusCode: 500, Category: "Retryable", Message: "Error in sending request"}
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &APIError{StatusCode: 500, Category: "Retryable", Message: "Error in reading response body"}
	}

	m.logger.Debugf("[Async Destination Manager] Marketo Get Lead Status Response: %v", string(body))

	if !m.checkForCSVLikeResponse(resp) {
		var marketoResponse MarketoResponse
		err = json.Unmarshal(body, &marketoResponse)
		if err != nil {
			return nil, &APIError{StatusCode: 500, Category: "Retryable", Message: "Error in parsing response body"}
		}

		statusCode, category, errorMessage := parseMarketoResponse(marketoResponse)
		// if the response is not a csv like response, then it should be a json response
		return nil, &APIError{StatusCode: statusCode, Category: category, Message: errorMessage}
	}

	// if the response is a csv like response
	// parse the csv response

	reader := csv.NewReader(strings.NewReader(string(body)))

	// read each row one by one
	rows := make([][]string, 0)
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			m.logger.Error("Error in parsing csv response", err.Error())
			m.statsFactory.NewStat("async_destination_manager.marketo_bulk_upload.csv_parse_error", stats.CountType).Increment()
			continue
		}
		rows = append(rows, row)
	}

	if len(rows) == 0 {
		return nil, &APIError{StatusCode: 500, Category: "Retryable", Message: "No data in csv response"}
	}

	// The first row is the header
	header := rows[0]

	records := make([]map[string]string, 0, len(rows)-1)

	for _, row := range rows[1:] {
		record := make(map[string]string)
		for i, value := range row {
			if i < len(header) {
				record[header[i]] = value
			}
		}
		records = append(records, record)
	}

	return records, nil
}

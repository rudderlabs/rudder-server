package marketobulkupload

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"time"
)

// Define configuration structure
type Config struct {
	MunchkinID          string
	DeDuplicationField  string
	ColumnFieldsMapping []ColumnField
}

type ColumnField struct {
	From string
	To   string
}

type FieldSchema struct {
	Name     string `json:"name"`
	DataType string `json:"dataType"`
}

type DescribeResponse struct {
	RequestId string `json:"requestId"`
	Success   bool   `json:"success"`
	Errors    []struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"errors"`
	Result []struct {
		Fields []FieldSchema `json:"fields"`
	} `json:"result"`
}

type InputEvent struct {
	Metadata Metadata `json:"metadata"`
	Message  Message  `json:"message"`
}

type Metadata struct {
	JobID string `json:"job_id"`
}

type Message map[string]interface{}

type TransformationError struct {
	Message string
	Status  int
}

func (e *TransformationError) Error() string {
	return e.Message
}

type ConfigurationError struct {
	Message string
}

func (e *ConfigurationError) Error() string {
	return e.Message
}

func fetchFieldSchemaNames(config Config, accessToken string) ([]string, error) {
	fieldSchemaMapping, err := getFieldSchemaMap(accessToken, config.MunchkinID)
	if err != nil {
		return nil, err
	}
	if len(fieldSchemaMapping) > 0 {
		fieldSchemaNames := make([]string, 0, len(fieldSchemaMapping))
		for fieldName := range fieldSchemaMapping {
			fieldSchemaNames = append(fieldSchemaNames, fieldName)
		}
		return fieldSchemaNames, nil
	}
	return nil, &RetryableError{"Failed to fetch Marketo Field Schema", 500, fieldSchemaMapping}
}

func getHeaderFields(config Config, fieldSchemaNames []string) ([]string, error) {
	columnFieldsMapping := config.ColumnFieldsMapping

	for _, colField := range columnFieldsMapping {
		if !contains(fieldSchemaNames, colField.To) {
			return nil, &ConfigurationError{fmt.Sprintf("The field %s is not present in Marketo Field Schema. Aborting", colField.To)}
		}
	}

	columnFieldMap := make(map[string]string)
	for _, colField := range columnFieldsMapping {
		columnFieldMap[colField.To] = colField.From
	}

	headerFields := make([]string, 0, len(columnFieldMap))
	for key := range columnFieldMap {
		headerFields = append(headerFields, key)
	}

	return headerFields, nil
}

func getFileData(inputEvents []InputEvent, config Config, headerArr []string) ([]byte, []string, []string, error) {
	startTime := time.Now()

	messageArr := make([]map[string]Message, len(inputEvents))
	for i, event := range inputEvents {
		jobID := event.Metadata.JobID
		data := map[string]Message{jobID: event.Message}
		messageArr[i] = data
	}

	if config.DeDuplicationField != "" {
		dedupMap := make(map[string][]int)
		for i, element := range inputEvents {
			dedupKey := element.Message[config.DeDuplicationField].(string)
			dedupMap[dedupKey] = append(dedupMap[dedupKey], i)
		}

		for _, indexes := range dedupMap {
			firstBorn := make(Message)
			for _, idx := range indexes {
				for _, headerStr := range headerArr {
					if val, ok := inputEvents[idx].Message[headerStr]; ok {
						firstBorn[headerStr] = val
					}
				}
			}
			inputEvents[indexes[0]].Message = firstBorn
		}
	}

	csvBuffer := &bytes.Buffer{}
	writer := csv.NewWriter(csvBuffer)
	writer.Write(headerArr)

	unsuccessfulJobs := []string{}
	successfulJobs := []string{}
	marketoFilePath := getMarketoFilePath()

	for _, row := range messageArr {
		csvRow := make([]string, len(headerArr))
		for i, fieldName := range headerArr {
			csvRow[i] = fmt.Sprintf("%v", row[fieldName])
		}

		csvSize := csvBuffer.Len()
		if csvSize <= 10*1024*1024 {
			writer.Write(csvRow)
			for jobID := range row {
				successfulJobs = append(successfulJobs, jobID)
			}
		} else {
			for jobID := range row {
				unsuccessfulJobs = append(unsuccessfulJobs, jobID)
			}
		}
	}

	writer.Flush()

	fileSize := csvBuffer.Len()
	if fileSize > 1 {
		startTime = time.Now()
		if err := ioutil.WriteFile(marketoFilePath, csvBuffer.Bytes(), 0644); err != nil {
			return nil, nil, nil, err
		}

		readStream, err := ioutil.ReadFile(marketoFilePath)
		if err != nil {
			return nil, nil, nil, err
		}
		os.Remove(marketoFilePath)

		return readStream, successfulJobs, unsuccessfulJobs, nil
	}

	return nil, successfulJobs, unsuccessfulJobs, nil
}

func getImportID(input []InputEvent, config Config, accessToken string, csvHeader []string) (string, []string, []string, error) {
	readStream, successfulJobs, unsuccessfulJobs, err := getFileData(input, config, csvHeader)
	if err != nil {
		return "", nil, nil, &TransformationError{fmt.Sprintf("Marketo File Upload: Error while creating file: %v", err), 500}
	}

	if readStream != nil {
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)
		writer.WriteField("format", "csv")
		part, err := writer.CreateFormFile("file", "marketo_bulk_upload.csv")
		if err != nil {
			return "", nil, nil, err
		}
		part.Write(readStream)
		writer.WriteField("access_token", accessToken)
		writer.Close()

		req, err := http.NewRequest("POST", fmt.Sprintf("https://%s.mktorest.com/bulk/v1/leads.json", config.MunchkinID), body)
		if err != nil {
			return "", nil, nil, err
		}
		req.Header.Set("Content-Type", writer.FormDataContentType())

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			return "", nil, nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			respBody, _ := ioutil.ReadAll(resp.Body)
			return "", nil, nil, &TransformationError{fmt.Sprintf("Unable to upload file due to error: %s", string(respBody)), resp.StatusCode}
		}

		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return "", nil, nil, err
		}

		var fileUploadResponse struct {
			ImportID string `json:"importId"`
		}
		if err := json.Unmarshal(respBody, &fileUploadResponse); err != nil {
			return "", nil, nil, err
		}

		return fileUploadResponse.ImportID, successfulJobs, unsuccessfulJobs, nil
	}

	return "", successfulJobs, unsuccessfulJobs, nil
}

func responseHandler(input []InputEvent, config Config) (map[string]interface{}, error) {
	accessToken, err := getAccessToken(config)
	if err != nil {
		return nil, err
	}

	fieldSchemaNames, err := fetchFieldSchemaNames(config, accessToken)
	if err != nil {
		return nil, err
	}

	headerForCsv, err := getHeaderFields(config, fieldSchemaNames)
	if err != nil {
		return nil, err
	}

	if len(headerForCsv) == 0 {
		return nil, &ConfigurationError{
			Message: "Faulty configuration. Please map your traits to Marketo column fields",
		}
	}

	importId, successfulJobs, unsuccessfulJobs, err := getImportID(input, config, accessToken, headerForCsv)
	if err != nil {
		return nil, err
	}

	if importId != "" {
		csvHeader := headerForCsv
		metadata := map[string]interface{}{
			"successfulJobs":   successfulJobs,
			"unsuccessfulJobs": unsuccessfulJobs,
			"csvHeader":        csvHeader,
		}
		response := map[string]interface{}{
			"statusCode": 200,
			"importId":   importId,
			"metadata":   metadata,
		}
		return response, nil
	}

	return map[string]interface{}{
		"statusCode":   500,
		"FailedReason": "[Marketo File upload]: No import id received",
	}, nil
}

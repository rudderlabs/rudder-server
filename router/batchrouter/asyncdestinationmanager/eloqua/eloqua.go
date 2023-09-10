package eloqua

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
)

type EloquaServiceImpl struct {
	bulkApi string
}

func NewEloquaServiceImpl(version string) *EloquaServiceImpl {
	return &EloquaServiceImpl{
		bulkApi: fmt.Sprintf("/api/bulk/%v", version),
	}
}

func (e *EloquaServiceImpl) MakeHTTPRequest(data *HttpRequestData) ([]byte, int, error) {
	req, err := http.NewRequest(data.Method, data.Endpoint, data.Body)
	if err != nil {
		return nil, 500, err
	}
	req.Header.Add("Authorization", data.Authorization)
	req.Header.Add("content-type", data.ContentType)
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, 500, err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, 500, err
	}
	return body, res.StatusCode, err
}

func (e *EloquaServiceImpl) GetBaseEndpoint(data *HttpRequestData) (string, error) {
	data.Method = http.MethodGet
	data.Endpoint = "https://login.eloqua.com/id"

	body, _, err := e.MakeHTTPRequest(data)
	if err != nil {
		return "", err
	}
	loginDetailsResponse := LoginDetailsResponse{}
	err = json.Unmarshal(body, &loginDetailsResponse)
	if err != nil {
		return "", err
	}
	return loginDetailsResponse.Urls.Base, nil
}

func (e *EloquaServiceImpl) FetchFields(data *HttpRequestData) (*Fields, error) {
	var endpoint string
	if data.DynamicPart != "" {
		endpoint = data.BaseEndpoint + e.bulkApi + "/customObjects/" + data.DynamicPart + "/fields"
	} else {
		endpoint = data.BaseEndpoint + e.bulkApi + "/contacts/fields"
	}
	data.Method = http.MethodGet
	data.Endpoint = endpoint
	body, statusCode, err := e.MakeHTTPRequest(data)
	if err != nil {
		return nil, err
	}
	if statusCode != 200 {
		return nil, fmt.Errorf("either authorization is wrong or the object is not found")
	}
	unmarshalledBody := Fields{}
	err = json.Unmarshal(body, &unmarshalledBody)
	if err != nil {
		return nil, err
	}
	return &unmarshalledBody, nil
}

func (e *EloquaServiceImpl) CreateImportDefinition(data *HttpRequestData, eventType string) (*ImportDefinition, error) {
	var endpoint string
	if eventType == "track" {
		endpoint = data.BaseEndpoint + e.bulkApi + "/customObjects/" + data.DynamicPart + "/imports"
	} else {
		endpoint = data.BaseEndpoint + e.bulkApi + "/contacts/imports"
	}

	data.Method = http.MethodPost
	data.Endpoint = endpoint
	data.ContentType = "application/json"
	body, _, err := e.MakeHTTPRequest(data)
	if err != nil {
		return nil, err
	}
	unmarshalledBody := ImportDefinition{}
	err = json.Unmarshal(body, &unmarshalledBody)
	if err != nil {
		return nil, err
	}
	return &unmarshalledBody, nil
}

func (e *EloquaServiceImpl) UploadData(data *HttpRequestData, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	data.Endpoint = data.BaseEndpoint + e.bulkApi + data.DynamicPart + "/data"
	data.Method = http.MethodPost
	data.ContentType = "text/csv"
	data.Body = file
	_, statusCode, err := e.MakeHTTPRequest(data)
	if err != nil {
		return err
	}
	if statusCode != 204 {
		return fmt.Errorf("Upload failed with status code: %d", statusCode)
	}
	return nil
}

func (e *EloquaServiceImpl) UploadDataWithoutCSV(data *HttpRequestData, uploadData []map[string]interface{}) error {
	data.Endpoint = data.BaseEndpoint + e.bulkApi + data.DynamicPart + "/data"
	data.Method = http.MethodPost
	data.ContentType = "application/json"
	marshalledData, _ := json.Marshal(uploadData)
	data.Body = strings.NewReader(string(marshalledData))
	_, statusCode, err := e.MakeHTTPRequest(data)
	if err != nil {
		return err
	}
	if statusCode != 204 {
		return fmt.Errorf("Upload failed with status code: %d", statusCode)
	}
	return nil
}

func (e *EloquaServiceImpl) RunSync(data *HttpRequestData) (string, error) {
	data.Method = http.MethodPost
	data.ContentType = "application/json"
	data.Endpoint = data.BaseEndpoint + e.bulkApi + "/syncs"
	body, statusCode, err := e.MakeHTTPRequest(data)
	if statusCode != 201 {
		return "", fmt.Errorf("Upload failed with status code: %d", statusCode)
	}
	if err != nil {
		return "", err
	}
	unmarshalledBody := SyncResponse{}
	err = json.Unmarshal(body, &unmarshalledBody)
	if err != nil {
		return "", err
	}
	return unmarshalledBody.Uri, nil
}

func (e *EloquaServiceImpl) CheckSyncStatus(data *HttpRequestData) (string, error) {
	data.Method = http.MethodGet
	data.ContentType = "application/json"
	data.Endpoint = data.BaseEndpoint + e.bulkApi + data.DynamicPart

	body, statusCode, err := e.MakeHTTPRequest(data)
	if statusCode != 200 {
		return "", fmt.Errorf("Upload failed with status code: %d", statusCode)
	}
	if err != nil {
		return "", err
	}
	unmarshalledBody := SyncStatusResponse{}
	err = json.Unmarshal(body, &unmarshalledBody)
	if err != nil {
		return "", err
	}
	return unmarshalledBody.Status, nil
}

func (e *EloquaServiceImpl) CheckRejectedData(data *HttpRequestData) (*RejectResponse, error) {
	data.Method = http.MethodGet
	data.Endpoint = data.BaseEndpoint + e.bulkApi + data.DynamicPart + "/rejects?offset=" + strconv.Itoa(data.Offset)
	data.ContentType = "application/json"
	body, statusCode, err := e.MakeHTTPRequest(data)
	if statusCode != 200 {
		return nil, fmt.Errorf("unable to delete the import definition with status code: %d", statusCode)
	}
	if err != nil {
		return nil, err
	}
	unmarshalledBody := RejectResponse{}
	err = json.Unmarshal(body, &unmarshalledBody)
	if err != nil {
		return nil, err
	}
	return &unmarshalledBody, nil
}

func (e *EloquaServiceImpl) DeleteImportDefinition(data *HttpRequestData) error {
	data.Method = http.MethodDelete
	data.Endpoint = data.BaseEndpoint + e.bulkApi + data.DynamicPart
	_, statusCode, err := e.MakeHTTPRequest(data)
	if err != nil {
		return err
	}
	if statusCode != 204 {
		return fmt.Errorf("unable to delete the import definition with status code: %d", statusCode)
	}
	return nil
}

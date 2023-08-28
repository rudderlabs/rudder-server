package eloqua

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
)

type implementEloqua struct {
}

func (e *implementEloqua) MakeHTTPRequest(data *HttpRequestData) ([]byte, int, error) {
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

func (e *implementEloqua) GetBaseEndpoint(data *HttpRequestData) (string, error) {

	data.Method = "GET"
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

func (e *implementEloqua) FetchFields(data *HttpRequestData) (*Fields, error) {
	var endpoint string
	if data.DynamicPart != "" {
		endpoint = data.BaseEndpoint + "/api/bulk/2.0/customObjects/" + data.DynamicPart + "/fields"
	} else {
		endpoint = data.BaseEndpoint + "/api/bulk/2.0/contacts/fields"
	}
	data.Method = "GET"
	data.Endpoint = endpoint
	body, _, err := e.MakeHTTPRequest(data)
	if err != nil {
		return nil, err
	}
	unmarshalledBody := Fields{}
	err = json.Unmarshal(body, &unmarshalledBody)
	if err != nil {
		return nil, err
	}
	return &unmarshalledBody, nil
}

func (e *implementEloqua) CreateImportDefinition(data *HttpRequestData, eventType string) (*ImportDefinition, error) {
	var endpoint string
	if eventType == "track" {
		endpoint = data.BaseEndpoint + "/api/bulk/2.0/customObjects/" + data.DynamicPart + "/imports"
	} else {
		endpoint = data.BaseEndpoint + "/api/bulk/2.0/contacts/imports"
	}

	data.Method = "POST"
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

func (e *implementEloqua) UploadData(data *HttpRequestData, filePath string) error {

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	data.Endpoint = data.BaseEndpoint + "/api/bulk/2.0" + data.DynamicPart + "/data"
	data.Method = "POST"
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

func (e *implementEloqua) RunSync(data *HttpRequestData) (string, error) {
	data.Method = "POST"
	data.ContentType = "application/json"
	data.Endpoint = data.BaseEndpoint + "/api/bulk/2.0/syncs"
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

func (e *implementEloqua) CheckSyncStatus(data *HttpRequestData) (string, error) {
	data.Method = "GET"
	data.ContentType = "application/json"
	data.Endpoint = data.BaseEndpoint + "/api/bulk/2.0" + data.DynamicPart

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
		return "nil", err
	}
	return unmarshalledBody.Status, nil
}

func (e *implementEloqua) DeleteImportDefinition(data *HttpRequestData) error {
	data.Method = "DELETE"
	data.Endpoint = data.BaseEndpoint + "/api/bulk/2.0" + data.DynamicPart
	_, statusCode, err := e.MakeHTTPRequest(data)
	if err != nil {
		return err
	}
	if statusCode != 204 {
		return fmt.Errorf("unable to delete the import definition with status code: %d", statusCode)
	}
	return nil
}

func (e *implementEloqua) CheckRejectedData(data *HttpRequestData) (*RejectResponse, error) {
	data.Method = "GET"
	data.Endpoint = data.BaseEndpoint + "/api/bulk/2.0" + data.DynamicPart + "/rejects?offset=" + strconv.Itoa(data.Offset)
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

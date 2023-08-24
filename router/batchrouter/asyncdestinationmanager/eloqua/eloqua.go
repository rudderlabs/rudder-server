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

type Eloqua interface {
	FetchFields(*Data) (*Fields, error)
	CreateImportDefinition(*Data) (*Fields, error)
	UploadData(*Data) error
	RunSync(*Data) error
	DeleteImportDefinition(*Data) error
}

func getBaseEndpoint(data *Data) (string, error) {
	req, err := http.NewRequest("GET", "https://login.eloqua.com/id", nil)
	if err != nil {
		return "", err
	}
	req.Header.Add("Authorization", data.Authorization)
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
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

func FetchFields(data *Data) (*Fields, error) {
	var endpoint string
	if data.DynamicPart != "" {
		endpoint = data.BaseEndpoint + "/api/bulk/2.0/customObjects/" + data.DynamicPart + "/fields"
	} else {
		endpoint = data.BaseEndpoint + "/api/bulk/2.0/contacts/fields"
	}
	req, err := http.NewRequest("GET", endpoint, nil)
	req.Header.Set("Authorization", data.Authorization)
	if err != nil {
		return nil, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	unmarshalledBody := Fields{}
	json.Unmarshal(body, &unmarshalledBody)
	return &unmarshalledBody, nil
}

func CreateImportDefinition(data *Data, eventType string) (*ImportDefinition, error) {

	marshalledData, err := json.Marshal(data.Body)
	if err != nil {
		return nil, err
	}
	var endpoint string
	if eventType == "track" {
		endpoint = data.BaseEndpoint + "/api/bulk/2.0/customObjects/" + data.DynamicPart + "/imports"
	} else {
		endpoint = data.BaseEndpoint + "/api/bulk/2.0/contacts/imports"
	}

	req, err := http.NewRequest("POST", endpoint, strings.NewReader(string(marshalledData)))

	req.Header.Add("Authorization", data.Authorization)
	req.Header.Add("Content-Type", "application/json")
	if err != nil {
		return nil, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	unmarshalledBody := ImportDefinition{}
	json.Unmarshal(body, &unmarshalledBody)
	return &unmarshalledBody, nil
}

func UploadData(data *Data, filePath string) error {

	body1, err := os.Open(filePath)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", data.BaseEndpoint+"/api/bulk/2.0"+data.DynamicPart+"/data", body1)

	req.Header.Add("Authorization", data.Authorization)
	req.Header.Add("Content-Type", "text/csv")
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if res.StatusCode != 204 {
		return fmt.Errorf("Upload failed with status code: %d", res.StatusCode)
	}
	defer res.Body.Close()
	return nil
}

func RunSync(data *Data) (string, error) {
	marshalledData, err := json.Marshal(data.Body)
	body1 := strings.NewReader(string(marshalledData))
	req, err := http.NewRequest("POST", data.BaseEndpoint+"/api/bulk/2.0/syncs", body1)

	req.Header.Add("Authorization", data.Authorization)
	req.Header.Add("Content-Type", "application/json")
	if err != nil {
		return "", err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	if res.StatusCode != 201 {
		return "", fmt.Errorf("Upload failed with status code: %d", res.StatusCode)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	unmarshalledBody := SyncResponse{}
	json.Unmarshal(body, &unmarshalledBody)
	return unmarshalledBody.Uri, nil
}

func CheckSyncStatus(data *Data) (string, error) {
	req, err := http.NewRequest("GET", data.BaseEndpoint+"/api/bulk/2.0"+data.DynamicPart, nil)

	req.Header.Add("Authorization", data.Authorization)
	req.Header.Add("Content-Type", "application/json")
	if err != nil {
		return "", err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	if res.StatusCode != 200 {
		return "", fmt.Errorf("Upload failed with status code: %d", res.StatusCode)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	unmarshalledBody := SyncStatusResponse{}
	json.Unmarshal(body, &unmarshalledBody)
	return unmarshalledBody.Status, nil
}

func DeleteImportDefinition(data *Data) error {

	req, err := http.NewRequest("DELETE", data.BaseEndpoint+"/api/bulk/2.0"+data.DynamicPart, nil)

	req.Header.Set("Authorization", data.Authorization)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if res.StatusCode != 204 {
		return fmt.Errorf("unable to delete the import definition with status code: %d", res.StatusCode)
	}
	defer res.Body.Close()
	return nil
}

func CheckRejectedData(data *Data) (*RejectResponse, error) {
	req, err := http.NewRequest("GET", data.BaseEndpoint+"/api/bulk/2.0"+data.DynamicPart+"/rejects?offset="+strconv.Itoa(data.Offset), nil)
	req.Header.Set("Authorization", data.Authorization)
	req.Header.Add("Content-Type", "application/json")
	if err != nil {
		return nil, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("unable to delete the import definition with status code: %d", res.StatusCode)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	unmarshalledBody := RejectResponse{}
	json.Unmarshal(body, &unmarshalledBody)
	return &unmarshalledBody, nil
}

package asyncfilemanager

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"net/http"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

// Upload passed in file to s3
func (manager *MarketoManager) Upload(url string, method string, filePath string) {
	payload := &bytes.Buffer{}
	writer := multipart.NewWriter(payload)
	err := writer.WriteField("format", "csv")
	if err != nil {
		panic(err)
	}
	err = writer.WriteField("file", fmt.Sprintf("@%s", filePath))
	if err != nil {
		panic(err)
	}
	err = writer.Close()
	if err != nil {
		panic(err)
	}

	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		panic(err)
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Marketo-Account-ID", manager.Config.AccountID)
	req.Header.Set("Marketo-Client-ID", manager.Config.CliendID)
	req.Header.Set("Marketo-Client-Secret", manager.Config.ClientSecret)

	res, err := client.Do(req)
	if err != nil {
		panic(err)
	}

	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}

type MarketoManager struct {
	Config *MarketoConfig
	JobsDB jobsdb.JobsDB
}

func GetMarketoConfig(config map[string]interface{}) *MarketoConfig {
	var accountID, clientID, clientSecret string
	if config["accountid"] != nil {
		accountID = config["accountid"].(string)
	}
	if config["clientid"] != nil {
		clientID = config["clientid"].(string)
	}
	if config["clientsecret"] != nil {
		clientSecret = config["clientsecret"].(string)
	}
	columnsInterface := config["columns"].([]interface{})
	columns := make([]string, len(columnsInterface))
	for i, v := range columnsInterface {
		columns[i] = v.(string)
	}

	return &MarketoConfig{AccountID: accountID, CliendID: clientID, ClientSecret: clientSecret, Columns: columns}
}

type MarketoConfig struct {
	AccountID    string
	CliendID     string
	ClientSecret string
	Columns      []string
}

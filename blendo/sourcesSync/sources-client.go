package sourcesSync

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/rudderlabs/rudder-server/utils/sysUtils"
)

var (
	Http   sysUtils.HttpI   = sysUtils.NewHttp()
	IoUtil sysUtils.IoUtilI = sysUtils.NewIoUtil()
)

type SourcesClient struct {
	SourcesURL string
}

// PutConfigToRegistry puts config in blendo registry
func (br *SourcesClient) PutConfigToRegistry(id string, data interface{}) (response []byte, ok bool) {
	log.Infof("Putting to registry %s", id)
	return br.RequestToRegistry(id, "POST", data)
}

// DeleteConfigFromRegistry deletes config from blendo registry
func (br *SourcesClient) DeleteConfigFromRegistry(id string) (response []byte, ok bool) {
	log.Info("Deleting from registry")
	return br.RequestToRegistry(id, "DELETE", nil)
}

// RequestToRegistry sends request to registry
func (br *SourcesClient) RequestToRegistry(id string, method string, data interface{}) (response []byte, ok bool) {
	client := &http.Client{}
	url := fmt.Sprintf("%s/schedule/%s", br.SourcesURL, id)
	var request *http.Request
	var err error
	if data != nil {
		dataJSON, _ := json.Marshal(data)
		dataJSONReader := bytes.NewBuffer(dataJSON)
		request, err = Http.NewRequest(method, url, dataJSONReader)
	} else {
		request, err = Http.NewRequest(method, url, nil)
	}
	if err != nil {
		log.Errorf("BLENDO Registry: Failed to make %s request: %s, Error: %s", method, url, err.Error())
		return []byte{}, false
	}

	request.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(request)
	if err != nil {
		log.Errorf("BLENDO Registry: Failed to execute %s request: %s, Error: %s", method, url, err.Error())
		return []byte{}, false
	}
	if resp.StatusCode != 200 && resp.StatusCode != 202 && resp.StatusCode != 204 {
		log.Errorf("BLENDO Registry: Got error response %d", resp.StatusCode)
	}

	body, err := IoUtil.ReadAll(resp.Body)
	defer resp.Body.Close()

	log.Debugf("BLENDO Registry: Successful %s", string(body))
	return body, true
}

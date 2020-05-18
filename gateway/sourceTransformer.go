package gateway

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

type sourceTransformRespT struct {
	event    []byte
	writeKey string
}

func parseWriteKey(req *http.Request) (writeKey string, found bool) {
	queryParams := req.URL.Query()
	writeKeys, found := queryParams["writeKey"]
	if found {
		writeKey = writeKeys[0]
	} else {
		writeKey, _, found = req.BasicAuth()
	}
	return
}

func (gateway *HandleT) sourceTransform(req *http.Request) (sourceTransformRespT, error) {
	writeKey, ok := parseWriteKey(req)
	if !ok {
		return sourceTransformRespT{}, errors.New(NoWriteKeyInQueryParams)
	}
	configSubscriberLock.RLock()
	sourceDefName, ok := enabledWriteKeySourceDefMap[writeKey]
	configSubscriberLock.RUnlock()
	if !ok {
		return sourceTransformRespT{}, errors.New(getStatus(InvalidWriteKey))
	}

	body, err := ioutil.ReadAll(req.Body)
	req.Body.Close()

	if err != nil {
		return sourceTransformRespT{}, errors.New(RequestBodyReadFailed)
	}

	url := fmt.Sprintf(`%s/%s`, sourceTransformerURL, strings.ToLower(sourceDefName))
	resp, err := gateway.transformerClient.Post(url, "application/json; charset=utf-8",
		bytes.NewBuffer(body))

	if err != nil {
		return sourceTransformRespT{}, errors.New(getStatus(SourceTransformerFailed))
	}
	body, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return sourceTransformRespT{}, errors.New(getStatus(SourceTrasnformerResponseReadFailed))
	}
	if resp.StatusCode != 200 {
		return sourceTransformRespT{}, errors.New(string(body))
	}
	return sourceTransformRespT{event: body, writeKey: writeKey}, nil
}

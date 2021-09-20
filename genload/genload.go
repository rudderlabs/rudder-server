package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/segmentio/ksuid"
	"github.com/tidwall/sjson"
)

var (
	writeKeys    []string
	reqPerSecond int64
	loadTime     int64
	pkgLogger    logger.LoggerI
)

func main() {
	config.Load()
	logger.Init()
	secret := config.GetEnv("secret", "defaultSecret")
	reqPerSecond, _ = strconv.ParseInt(config.GetEnv("reqPerSecond", "1"), 10, 0)
	configBEURL := config.GetEnv("configBEURL", "https://api.rudderlabs.com")
	dataplaneURL := config.GetEnv("dataplaneURL", "http://localhost:8080/v1/batch")
	loadTime, _ = strconv.ParseInt(config.GetEnv("loadTime", "3600"), 10, 0)
	pkgLogger = logger.NewLogger().Child("genload")

	client := &http.Client{}
	req, err := http.NewRequest("GET", configBEURL, nil)
	if err != nil {
		pkgLogger.Errorf("Got error %s\n", err.Error())
	}

	//getting workspaces
	req.SetBasicAuth(secret, "")
	resp, err := client.Do(req)
	if err != nil {
		pkgLogger.Info("failed to get config for ", secret, ":", err.Error())
	}
	var respBody []byte
	if resp != nil && resp.Body != nil {
		respBody, _ = ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
	}
	pkgLogger.Info("got the config successfully")
	var workspaces backendconfig.WorkspacesT
	err = json.Unmarshal(respBody, &workspaces.WorkspaceSourcesMap)
	if err != nil {
		pkgLogger.Info("Error while parsing request", err)
	}
	pkgLogger.Info("done parsing the config")
	writeKeys = make([]string, 0)
	for _, workspaceConfig := range workspaces.WorkspaceSourcesMap {
		for _, source := range workspaceConfig.Sources {
			writeKeys = append(writeKeys, source.WriteKey)
		}
	}
	pkgLogger.Info("Got all the write keys")

	for _, wk := range writeKeys {
		go func(wk string) {
			sendRequests(wk, dataplaneURL)
		}(wk)
	}
	pkgLogger.Info("started goroutines to send requests from all the sources")
	time.Sleep(time.Duration(loadTime) * time.Second)
}

func sendRequests(writeKey, dataplaneURL string) {
	for {
		client := &http.Client{}
		userID := ksuid.New().String()
		payload, _ = sjson.SetBytes(payload, fmt.Sprintf(`batch.%v.anonymousId`, 0), userID)
		// payload, _ = sjson.SetBytes(payload, fmt.Sprintf(`batch.%v.anonymousId`, 0), userID) //change another fields - few more
		req, err := http.NewRequest("POST", dataplaneURL, bytes.NewBuffer(payload))
		if err != nil {
			pkgLogger.Errorf("error creating request: %s", err.Error())
		}
		req.Header.Add("Authorization", "Basic "+basicAuth(writeKey, ""))
		_, err = client.Do(req)
		if err != nil {
			pkgLogger.Info(err.Error())
		}

		latency := 1000 / int(reqPerSecond)
		time.Sleep(time.Millisecond * time.Duration(latency))
	}
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

var data = `{
	"batch": [
	  {
		"anonymousId": "49e4bdd1c280bc00",
		"channel": "android-sdk",
		"destination_props": {
		  "AF": {
			"af_uid": "1566363489499-3377330514807116178"
		  }
		},
		"context": {
		  "app": {
			"build": "1",
			"name": "RudderAndroidClient",
			"namespace": "com.rudderlabs.android.sdk",
			"version": "1.0"
		  },
		  "device": {
			"id": "49e4bdd1c280bc00",
			"manufacturer": "Google",
			"model": "Android SDK built for x86",
			"name": "generic_x86"
		  },
		  "locale": "en-US",
		  "network": {
			"carrier": "Android"
		  },
		  "screen": {
			"density": 420,
			"height": 1794,
			"width": 1080
		  },
		  "traits": {
			"anonymousId": "49e4bdd1c280bc00"
		  },
		  "user_agent": "Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"
		},
		"event": "Demo Track",
		"integrations": {
		  "All": true
		},
		"properties": {
		  "label": "Demo Label",
		  "category": "Demo Category",
		  "value": 5
		},
		"type": "track",
		"originalTimestamp": "2019-08-12T05:08:30.909Z",
		"sentAt": "2019-08-12T05:08:30.909Z"
	  }
	]
  }
  `
var payload = []byte(data)

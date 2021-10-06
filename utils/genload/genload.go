package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/ksuid"
	"github.com/tidwall/sjson"
)

var (
	writeKeys     []string
	reqPerSecond  int64
	loadTime      int64
	poly          int64
	coefficients  []float64
	activeSources int64
)

type ConfigT struct {
	EnableMetrics   bool            `json:"enableMetrics"`
	WorkspaceID     string          `json:"workspaceId"`
	Sources         []SourceT       `json:"sources"`
	Libraries       LibrariesT      `json:"libraries"`
	ConnectionFlags ConnectionFlags `json:"flags"`
}

type ConnectionFlags struct {
	URL      string          `json:"url"`
	Services map[string]bool `json:"services"`
}

type SourceT struct {
	ID                         string
	Name                       string
	SourceDefinition           SourceDefinitionT
	Config                     map[string]interface{}
	Enabled                    bool
	WorkspaceID                string
	Destinations               []DestinationT
	WriteKey                   string
	DgSourceTrackingPlanConfig DgSourceTrackingPlanConfigT
}

type TrackingPlanT struct {
	Id      string `json:"id"`
	Version int    `json:"version"`
}

type DgSourceTrackingPlanConfigT struct {
	SourceId            string                            `json:"sourceId"`
	SourceConfigVersion int                               `json:"version"`
	Config              map[string]map[string]interface{} `json:"config"`
	MergedConfig        map[string]interface{}            `json:"mergedConfig"`
	Deleted             bool                              `json:"deleted"`
	TrackingPlan        TrackingPlanT                     `json:"trackingPlan"`
}

type TransformationT struct {
	VersionID string
	ID        string
	Config    map[string]interface{}
}

type LibraryT struct {
	VersionID string
}

type LibrariesT []LibraryT

type DestinationDefinitionT struct {
	ID            string
	Name          string
	DisplayName   string
	Config        map[string]interface{}
	ResponseRules map[string]interface{}
}

type SourceDefinitionT struct {
	ID       string
	Name     string
	Category string
}

type DestinationT struct {
	ID                    string
	Name                  string
	DestinationDefinition DestinationDefinitionT
	Config                map[string]interface{}
	Enabled               bool
	Transformations       []TransformationT
	IsProcessorEnabled    bool
}

//WorkspacesT holds sources of workspaces
type WorkspacesT struct {
	WorkspaceSourcesMap map[string]ConfigT `json:"-"`
}

func main() {
	secret := os.Getenv("SECRET")
	reqPerSecond, _ = strconv.ParseInt(os.Getenv("REQUESTS_PER_SECOND"), 10, 0)
	configBEURL := os.Getenv("CONFIG_BACKEND_URL")
	dataplaneURL := os.Getenv("DATAPLANE_URL")
	loadTime, _ = strconv.ParseInt(os.Getenv("LOAD_RUN_TIME"), 10, 0)
	activeSources, _ = strconv.ParseInt(os.Getenv("ACTIVE_SOURCES"), 10, 0)
	poly, _ = strconv.ParseInt(os.Getenv("POLYNOMIAL"), 10, 0)
	coeffsList := strings.Split(os.Getenv("COEFFICIENTS"), ",")
	skip, _ := strconv.ParseInt(os.Getenv("POLYNOMIAL"), 10, 0)
	coefficients = make([]float64, 0)
	for c := range coeffsList {
		coeff, _ := strconv.ParseFloat(coeffsList[c], 64)
		coefficients = append(coefficients, coeff)
	}
	fmt.Println(coefficients)

	client := &http.Client{}
	req, err := http.NewRequest("GET", configBEURL, nil)
	if err != nil {
		fmt.Printf("Got error %s\n", err.Error())
	}

	//getting workspaces
	req.SetBasicAuth(secret, "")
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("failed to get config for ", secret, ":", err.Error())
	}
	var respBody []byte
	if resp != nil && resp.Body != nil {
		respBody, _ = ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
	}
	fmt.Println("got the config successfully")
	var workspaces WorkspacesT
	err = json.Unmarshal(respBody, &workspaces.WorkspaceSourcesMap)
	if err != nil {
		fmt.Println("Error while parsing request", err)
		// fmt.Println(string(respBody))
	}
	fmt.Println("done parsing the config")
	writeKeys = make([]string, 0)
	for _, workspaceConfig := range workspaces.WorkspaceSourcesMap {
		for _, source := range workspaceConfig.Sources {
			writeKeys = append(writeKeys, source.WriteKey)
		}
	}
	fmt.Println("Got all the write keys")

	xShift, _ := strconv.ParseInt(os.Getenv("XSHIFT"), 10, 0)
	for j, writeKey := range writeKeys {
		requestGap := time.Duration(getRequestGap((j * int(skip)) + int(xShift)))
		go func(wk string, deltaT time.Duration) {
			sendRequests(wk, dataplaneURL, deltaT)
		}(writeKey, requestGap)
	}
	fmt.Println("started goroutines to send requests from all the sources")
	time.Sleep(time.Duration(loadTime) * time.Second)
}

func sendRequests(writeKey, dataplaneURL string, deltaT time.Duration) {
	for {
		client := &http.Client{}
		userID := ksuid.New().String()
		payload, _ = sjson.SetBytes(payload, fmt.Sprintf(`batch.%v.anonymousId`, 0), userID)
		// payload, _ = sjson.SetBytes(payload, fmt.Sprintf(`batch.%v.anonymousId`, 0), userID) //change another fields - few more
		req, err := http.NewRequest("POST", dataplaneURL, bytes.NewBuffer(payload))
		if err != nil {
			fmt.Printf("error creating request: %s\n", err.Error())
		}
		req.Header.Add("Authorization", "Basic "+basicAuth(writeKey, ""))
		_, err = client.Do(req)
		if err != nil {
			fmt.Println(err.Error())
		}

		time.Sleep(deltaT)
	}
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

func getRequestGap(j int) float64 {
	var rps float64
	for i := 0; i <= int(poly); i++ {
		rps += coefficients[int(poly)-i] * math.Pow(float64(j), float64(i))
	}

	return 604800000000000 / math.Pow(math.E, rps)
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

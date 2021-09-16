package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	writeKeys    []string
	reqPerSecond int64
	loadTime     int64
	pkgLogger    logger.LoggerI
)

func main() {
	secret := os.Args[1]
	reqPerSecond, _ = strconv.ParseInt(os.Args[2], 10, 0)
	configBEUrl := os.Args[3]
	dataplaneURL := os.Args[4]
	loadTime, _ = strconv.ParseInt(os.Args[5], 10, 0)

	configURL := configBEUrl + `/hostedWorkspaceConfig?fetchAll=true?fetchAll=true`

	client := &http.Client{}
	req, err := http.NewRequest("GET", configURL, nil)
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
	go func() {
		for {
			cmd := &exec.Cmd{
				Path:   "../scripts/generate-event",
				Args:   []string{"../scripts/generate-event", writeKey, dataplaneURL},
				Stdout: os.Stdout,
				Stderr: os.Stdout,
			}
			cmd.Run()
			latency := 1000 / int(reqPerSecond)
			time.Sleep(time.Millisecond * time.Duration(latency))
		}
	}()
}

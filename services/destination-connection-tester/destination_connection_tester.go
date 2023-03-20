package destination_connection_tester

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	configBackendURL              string
	maxRetry                      int
	retrySleep                    time.Duration
	instanceID                    string
	rudderConnectionTestingFolder string
	pkgLogger                     logger.Logger
)

const (
	destinationConnectionTesterEndpoint = "dataplane/testConnectionResponse"
	testPayload                         = "ok"
)

type DestinationConnectionTesterResponse struct {
	DestinationId string    `json:"destinationId"`
	InstanceId    string    `json:"instanceId"`
	Error         string    `json:"error"`
	TestedAt      time.Time `json:"testedAt"`
}

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("destination-connection-tester")
}

func loadConfig() {
	configBackendURL = config.GetString("CONFIG_BACKEND_URL", "https://api.rudderstack.com")
	maxRetry = config.GetInt("DestinationConnectionTester.maxRetry", 3)
	config.RegisterDurationConfigVariable(100, &retrySleep, false, time.Millisecond, []string{"DestinationConnectionTester.retrySleep", "DestinationConnectionTester.retrySleepInMS"}...)
	instanceID = config.GetString("INSTANCE_ID", "1")
	rudderConnectionTestingFolder = config.GetString("RUDDER_CONNECTION_TESTING_BUCKET_FOLDER_NAME", misc.RudderTestPayload)
}

func UploadDestinationConnectionTesterResponse(testResponse, destinationId string) {
	payload := DestinationConnectionTesterResponse{
		Error:         testResponse,
		TestedAt:      time.Now(),
		DestinationId: destinationId,
		InstanceId:    instanceID,
	}
	url := fmt.Sprintf("%s/%s", configBackendURL, destinationConnectionTesterEndpoint)
	if err := makePostRequest(url, payload); err != nil {
		pkgLogger.Errorf("failed to send destination connection response: %v", err)
	}
}

func makePostRequest(url string, payload interface{}) error {
	rawJSON, err := json.Marshal(payload)
	if err != nil {
		pkgLogger.Errorf("[Destination Connection Tester] Failed to marshal payload. Err: %v", err)
		return err
	}
	client := &http.Client{Timeout: config.GetDuration("HttpClient.dct.timeout", 30, time.Second)}
	retryCount := 0
	var resp *http.Response
	// Sending destination connection test response to Config Backend
	for {
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(rawJSON))
		if err != nil {
			pkgLogger.Errorf("[Destination Connection Tester] Failed to create new request. Err: %v", err)
			return err
		}
		req.Header.Set("Content-Type", "application/json;charset=UTF-8")
		req.SetBasicAuth(config.GetWorkspaceToken(), "")

		resp, err = client.Do(req)
		if err == nil {
			func() { httputil.CloseResponse(resp) }()
			break
		}

		pkgLogger.Errorf("DCT: Config Backend connection error", err)
		if retryCount > maxRetry {
			pkgLogger.Error("DCT: max retries exceeded trying to connect to config backend")
			return err
		}
		retryCount++
		time.Sleep(retrySleep)
	}

	if !(resp.StatusCode == http.StatusOK) {
		pkgLogger.Errorf("DCT: response Error from Config Backend: Status: %v, Body: %v ", resp.StatusCode, resp.Body)
	}
	return nil
}

func createTestFileForBatchDestination(destinationID string) string {
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to create tmp dir for testing this destination id %s: err %v", destinationID, err)
		panic(err)
	}

	gzipFilePath := fmt.Sprintf("%v/%v/%v.%v.%v.csv.gz", tmpDirPath, rudderConnectionTestingFolder, destinationID, uuid.New(), time.Now().Unix())
	err = os.MkdirAll(filepath.Dir(gzipFilePath), os.ModePerm)
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to make dir %s for testing this destination id %s: err %v", gzipFilePath, destinationID, err)
		panic(err)
	}
	gzWriter, err := misc.CreateGZ(gzipFilePath)
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to create gzip writer for testing this destination id %s: err %v", gzipFilePath, destinationID, err)
		panic(err)
	}
	if err = gzWriter.WriteGZ(testPayload); err != nil {
		panic(err)
	}
	if err = gzWriter.CloseGZ(); err != nil {
		panic(err)
	}
	return gzipFilePath
}

func uploadTestFileForBatchDestination(filename string, keyPrefixes []string, provider string, destination backendconfig.DestinationT) (objectName string, err error) {
	uploader, err := filemanager.DefaultFileManagerFactory.New(&filemanager.SettingsT{
		Provider: provider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         provider,
			Config:           destination.Config,
			UseRudderStorage: misc.IsConfiguredToUseRudderObjectStorage(destination.Config),
			WorkspaceID:      destination.WorkspaceID,
		}),
	})
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to initiate filemanager config for testing this destination id %s: err %v", destination.ID, err)
		panic(err)
	}
	uploadFile, err := os.Open(filename)
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to open file %s for testing this destination id %s: err %v", filename, destination.ID, err)
		panic(err)
	}
	defer misc.RemoveFilePaths(filename)
	defer func() { _ = uploadFile.Close() }()
	uploadOutput, err := uploader.Upload(context.TODO(), uploadFile, keyPrefixes...)
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to upload test file %s for testing this destination id %s: err %v", filename, destination.ID, err)
	}
	return uploadOutput.ObjectName, err
}

func TestBatchDestinationConnection(destination backendconfig.DestinationT) string {
	testFileName := createTestFileForBatchDestination(destination.ID)
	keyPrefixes := []string{config.GetString("RUDDER_CONNECTION_TESTING_BUCKET_FOLDER_NAME", misc.RudderTestPayload), destination.ID, time.Now().Format("01-02-2006")}
	_, err := uploadTestFileForBatchDestination(testFileName, keyPrefixes, destination.DestinationDefinition.Name, destination)
	if err != nil {
		return err.Error()
	}
	return ""
}

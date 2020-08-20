package destination_connection_tester

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/warehousemanager"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	uuid "github.com/satori/go.uuid"
)

var (
	configBackendURL              string
	maxRetry                      int
	retrySleep                    time.Duration
	instanceID                    string
	rudderConnectionTestingFolder string
)

const destinationConnectionTesterEndpoint = "dataplane/testConnectionResponse"
const testPayload = "ok"

type DestinationConnectionTesterResponse struct {
	DestinationId string    `json:"destinationId"`
	InstanceId    string    `json:"instanceId"`
	Error         string    `json:"error"`
	TestedAt      time.Time `json:"testedAt"`
}

func init() {
	loadConfig()
}

func loadConfig() {
	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderstack.com")
	maxRetry = config.GetInt("DestinationConnectionTester.maxRetry", 3)
	retrySleep = config.GetDuration("DestinationConnectionTester.retrySleepInMS", time.Duration(100)) * time.Millisecond
	instanceID = config.GetEnv("INSTANCE_ID", "1")
	rudderConnectionTestingFolder = config.GetEnv("RUDDER_CONNECTION_TESTING_BUCKET_FOLDER_NAME", "rudder-test-payload")

}

func UploadDestinationConnectionTesterResponse(testResponse string, destinationId string) {
	payload := DestinationConnectionTesterResponse{
		Error:         testResponse,
		TestedAt:      time.Now(),
		DestinationId: destinationId,
		InstanceId:    misc.GetNodeID(),
	}
	url := fmt.Sprintf("%s/%s", configBackendURL, destinationConnectionTesterEndpoint)
	if err := makePostRequest(url, payload); err != nil {
		logger.Errorf("failed to send destination connection response: %v", err)
	}
}

func makePostRequest(url string, payload interface{}) error {
	rawJSON, err := json.Marshal(payload)
	if err != nil {
		logger.Debugf(string(rawJSON))
		misc.AssertErrorIfDev(err)
		return err
	}
	client := &http.Client{}
	retryCount := 0
	var resp *http.Response
	//Sending destination connection test response to Config Backend
	for {
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(rawJSON))
		if err != nil {
			misc.AssertErrorIfDev(err)
			return err
		}
		req.Header.Set("Content-Type", "application/json;charset=UTF-8")
		req.SetBasicAuth(config.GetWorkspaceToken(), "")

		resp, err = client.Do(req)
		if err == nil {
			break
		}
		logger.Errorf("DCT: Config Backend connection error", err)
		if retryCount > maxRetry {
			logger.Error("DCT: max retries exceeded trying to connect to config backend")
			return err
		}
		retryCount++
		time.Sleep(retrySleep)
	}

	if !(resp.StatusCode == http.StatusOK) {
		logger.Errorf("DCT: response Error from Config Backend: Status: %v, Body: %v ", resp.StatusCode, resp.Body)
	}
	return nil
}

func createTestFileForBatchDestination(destinationID string) string {
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		logger.Errorf("DCT: Failed to create tmp dir for testing this destination id %s: err %v", destinationID, err)
		panic(err)
	}

	gzipFilePath := fmt.Sprintf("%v/%v/%v.%v.%v.csv.gz", tmpDirPath, rudderConnectionTestingFolder, destinationID, uuid.NewV4(), time.Now().Unix())
	err = os.MkdirAll(filepath.Dir(gzipFilePath), os.ModePerm)
	if err != nil {
		logger.Errorf("DCT: Failed to make dir %s for testing this destination id %s: err %v", gzipFilePath, destinationID, err)
		panic(err)
	}
	gzWriter, err := misc.CreateGZ(gzipFilePath)
	if err != nil {
		logger.Errorf("DCT: Failed to create gzip writer for testing this destination id %s: err %v", gzipFilePath, destinationID, err)
		panic(err)
	}
	gzWriter.WriteGZ(testPayload)
	gzWriter.CloseGZ()
	return gzipFilePath
}

func uploadTestFileForBatchDestination(filename string, keyPrefixes []string, provider string, destination backendconfig.DestinationT) (err error) {
	uploader, err := filemanager.New(&filemanager.SettingsT{
		Provider: provider,
		Config:   misc.GetObjectStorageConfig(provider, destination.Config),
	})
	if err != nil {
		logger.Errorf("DCT: Failed to initiate filemanager config for testing this destination id %s: err %v", destination.ID, err)
		panic(err)
	}
	uploadFile, err := os.Open(filename)
	if err != nil {
		logger.Errorf("DCT: Failed to open file %s for testing this destination id %s: err %v", filename, destination.ID, err)
		panic(err)
	}
	defer misc.RemoveFilePaths(filename)
	defer uploadFile.Close()
	_, err = uploader.Upload(uploadFile, keyPrefixes...)
	if err != nil {
		logger.Errorf("DCT: Failed to upload test file %s for testing this destination id %s: err %v", filename, destination.ID, err)
	}
	return err
}

func downloadTestFileForBatchDestination(testObjectKey string, provider string, destination backendconfig.DestinationT) (err error) {
	downloader, err := filemanager.New(&filemanager.SettingsT{
		Provider: provider,
		Config:   misc.GetObjectStorageConfig(provider, destination.Config),
	})
	if err != nil {
		logger.Errorf("DCT: Failed to initiate filemanager config for testing this destination id %s: err %v", destination.ID, err)
		panic(err)
	}

	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	testFilePath := fmt.Sprintf("%v/%v/%v.%v.%v.csv.gz", tmpDirPath, rudderConnectionTestingFolder, destination.ID, uuid.NewV4(), time.Now().Unix())
	err = os.MkdirAll(filepath.Dir(testFilePath), os.ModePerm)
	testFile, err := os.Create(testFilePath)
	if err != nil {
		panic(err)
	}
	err = downloader.Download(testFile, testObjectKey)
	if err != nil {
		logger.Errorf("DCT: Failed to download test file %s for testing this destination id %s: err %v", testObjectKey, destination.ID, err)
	}
	testFile.Close()
	misc.RemoveFilePaths(testFilePath)
	return err

}

func TestBatchDestinationConnection(destination backendconfig.DestinationT) string {
	testFileName := createTestFileForBatchDestination(destination.ID)
	keyPrefixes := []string{config.GetEnv("RUDDER_CONNECTION_TESTING_BUCKET_FOLDER_NAME", "rudder-test-payload"), destination.ID, time.Now().Format("01-02-2006")}
	err := uploadTestFileForBatchDestination(testFileName, keyPrefixes, destination.DestinationDefinition.Name, destination)
	var error string
	if err != nil {
		error = err.Error()
	}
	return error
}

func TestWarehouseDestinationConnection(destination backendconfig.DestinationT) string {
	provider := destination.DestinationDefinition.Name
	whManager, err := warehousemanager.NewWhManager(provider)
	if err != nil {
		panic(err)
	}
	testFileNameWithPath := createTestFileForBatchDestination(destination.ID)
	storageProvider := warehouseutils.ObjectStorageType(destination.DestinationDefinition.Name, destination.Config)
	keyPrefixes := []string{rudderConnectionTestingFolder, destination.ID, time.Now().Format("01-02-2006")}
	err = uploadTestFileForBatchDestination(testFileNameWithPath, keyPrefixes, storageProvider, destination)
	if err != nil {
		return err.Error()
	}
	fileSplit := strings.Split(testFileNameWithPath, "/")
	keyName := strings.Join(append(keyPrefixes, fileSplit[len(fileSplit)-1]), "/")
	err = downloadTestFileForBatchDestination(keyName, storageProvider, destination)
	if err != nil {
		return err.Error()
	}
	err = whManager.TestConnection(warehouseutils.ConfigT{
		Warehouse: warehouseutils.WarehouseT{
			Destination: destination,
		},
	})
	if err != nil {
		return err.Error()
	}
	return ""
}

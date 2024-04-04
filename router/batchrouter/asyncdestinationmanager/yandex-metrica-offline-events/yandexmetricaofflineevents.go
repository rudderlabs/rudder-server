package yandexmetricaofflineevents

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/yandex-metrica-offline-events/augmenter"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	oauthv2common "github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	oauthv2httpclient "github.com/rudderlabs/rudder-server/services/oauth/v2/http"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/gjson"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const bufferSize = 5000 * 1024

type YandexMetricaBulkUploader struct {
	destName          string
	destinationConfig map[string]interface{}
	transformUrl      string
	pollUrl           string
	logger            logger.Logger
	timeout           time.Duration
	client            *http.Client
}

func NewManager(destination *backendconfig.DestinationT, backendConfig backendconfig.BackendConfig) (*YandexMetricaBulkUploader, error) {
	yandexUploadManager := &YandexMetricaBulkUploader{
		destName:          destination.DestinationDefinition.Name,
		destinationConfig: destination.Config,
		pollUrl:           "",
		transformUrl:      config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090"),
		logger:            logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("YandexMetrica").Child("YandexMetricaBulkUploader"),
		timeout:           config.GetDuration("HttpClient.yandexMetricaBulkUpload.timeout", 30, time.Second),
	}
	cache := oauthv2.NewCache()
	optionalArgs := &oauthv2httpclient.HttpClientOptionalArgs{
		Logger:    logger.NewLogger().Child("YandexMetricOfflineEvents"),
		Augmenter: augmenter.YandexReqAugmenter,
	}
	// TODO: Add http related timeout values
	originalHttpClient := &http.Client{Transport: &http.Transport{}}
	// This client is used for Router Transformation using oauthV2
	yandexUploadManager.client = oauthv2httpclient.NewOAuthHttpClient(
		originalHttpClient,
		oauthv2common.RudderFlowDelivery,
		&cache,
		backendConfig,
		augmenter.GetAuthErrorCategoryForYandex,
		optionalArgs,
	)
	return yandexUploadManager, nil
}

// return a success response for the poll request every time by default
func (ym *YandexMetricaBulkUploader) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
	return common.PollStatusResponse{
		Complete:       true,
		InProgress:     false,
		StatusCode:     200,
		HasFailed:      false,
		HasWarning:     false,
		FailedJobURLs:  "",
		WarningJobURLs: "",
		Error:          "",
	}
}

type YandexMetricaOfflineEvents struct {
	// Add your configuration fields here
}

func NewYandexMetricaOfflineEvents() *YandexMetricaOfflineEvents {
	// Initialize and return a new instance of YandexMetricaOfflineEvents
	return &YandexMetricaOfflineEvents{}
}

func (ym *YandexMetricaBulkUploader) GetUploadStats(UploadStatsInput common.GetUploadStatsInput) common.GetUploadStatsResponse {
	return common.GetUploadStatsResponse{}
}

func generateCSVFromJSON(jsonData []byte, csvFilePath string) (string, error) {
	// Define an empty map to store the parsed JSON data
	var data map[string]interface{}

	// Unmarshal the JSON data into the map
	err := json.Unmarshal(jsonData, &data)
	if err != nil {
		return "", err
	}

	// Open the CSV file for writing
	localTmpDirName := fmt.Sprintf(`/%s/`, misc.RudderAsyncDestinationLogs)
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return "", err
	}
	folderPath := path.Join(tmpDirPath, localTmpDirName)
	_, e := os.Stat(folderPath)
	if os.IsNotExist(e) {
		folderPath, _ = os.MkdirTemp(folderPath, "")
	}
	path := path.Join(folderPath, uuid.NewString())
	// path := path.Join(tmpDirPath, localTmpDirName, uuid.NewString())
	csvFilePath = fmt.Sprintf(`%v.csv`, path)
	csvFile, err := os.Create(csvFilePath)
	_, _ = csvFile.Seek(0, 0)
	scanner := bufio.NewScanner(csvFile)
	scanner.Buffer(nil, bufferSize)

	if err != nil {
		return "", err
	}
	defer csvFile.Close()

	// Create a CSV writer
	csvWriter := csv.NewWriter(csvFile)

	// Define the header row based on key presence in "message" object
	var header []string
	var idDecider string
	if message, ok := data["input"].([]interface{})[0].(map[string]interface{})["message"].(map[string]interface{}); ok {
		if _, ok := message["ClientId"]; ok {
			header = []string{"ClientId", "Target", "DateTime", "Price", "Currency"}
			idDecider = "ClientId"
		} else if _, ok := message["Yclid"]; ok {
			header = []string{"Yclid", "Target", "DateTime", "Price", "Currency"}
			idDecider = "Yclid"
		} else if _, ok := message["UserId"]; ok {
			header = []string{"UserId", "Target", "DateTime", "Price", "Currency"}
			idDecider = "UserId"
		} else {
			return "", fmt.Errorf("missing 'ClientId', 'Yclid', or 'UserId' key in 'message' object")
		}
	} else {
		return "", fmt.Errorf("error accessing 'message' object in data")
	}

	// Write the header row
	err = csvWriter.Write(header)
	if err != nil {
		return "", err
	}

	// Extract and write data rows
	for _, element := range data["input"].([]interface{}) {
		message := element.(map[string]interface{})["message"].(map[string]interface{})
		// metadata := element.(map[string]interface{})["metadata"].(map[string]interface{})
		row := []string{
			fmt.Sprintf("%v", message[idDecider]),
			fmt.Sprintf("%v", message["Target"]),
			fmt.Sprintf("%v", message["DateTime"]),
			fmt.Sprintf("%v", message["Price"]),
			fmt.Sprintf("%v", message["Currency"]),
		}
		err = csvWriter.Write(row)
		if err != nil {
			return "", err
		}
	}

	// Flush the writer
	csvWriter.Flush()

	// Return the chosen header
	println("Header: ", header[0])
	return header[0], nil
}

func (ym *YandexMetricaBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	fmt.Println("Reached Upload Fn")
	destination := asyncDestStruct.Destination
	destinationID := destination.ID
	filePath := asyncDestStruct.FileName
	destConfig, err := json.Marshal(destination.Config)
	destConfigJson := string(destConfig)
	// extract counterId from destConfigJson as a string value
	counterId := gjson.Get(destConfigJson, "counterId").String()
	println("Counter ID: ", counterId)
	println("Goal ID: ", gjson.Get(destConfigJson, "goalId").String())
	destType := destination.DestinationDefinition.Name
	failedJobIDs := asyncDestStruct.FailedJobIDs
	importingJobIDs := asyncDestStruct.ImportingJobIDs
	// println("Counter ID: ", string(counterId))

	// destinationUploadUrl := asyncDestStruct.DestinationUploadURL
	// uploadURL, err := "https://api-metrica.yandex.net/management/v1/counter/{id}/offline_conversions/upload?client_id_type=USER_ID"
	file, err := os.Open(filePath)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedReason:  "Error while opening file",
			DestinationID: destinationID,
		}
	}
	defer file.Close()
	var input []common.AsyncJob
	decoder := json.NewDecoder(file)

	for decoder.More() {
		var tempJob common.AsyncJob
		err := decoder.Decode(&tempJob)
		if err != nil {
			return common.AsyncUploadOutput{
				FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
				FailedReason:  "BRT: Error in Unmarshalling Job for Yandex Metrica destination: " + err.Error(),
				FailedCount:   len(failedJobIDs) + len(importingJobIDs),
				DestinationID: destinationID,
			}
		}
		input = append(input, tempJob)
	}
	ympayload, err := json.Marshal(common.AsyncUploadT{
		Input:    input,
		Config:   destination.Config,
		DestType: strings.ToLower(destType),
	})
	// fmt.Println("Payload", string(ympayload))
	ym.logger.Info("Payload", string(ympayload))

	var csvFilePath string
	userId, err := generateCSVFromJSON(ympayload, csvFilePath)

	// println("user ID: ", userId)
	var uploadURL string
	if userId == "ClientId" {
		uploadURL, err = url.JoinPath("https://api-metrica.yandex.net/management/v1/counter/", counterId, "/offline_conversions/upload?client_id_type=CLIENT_ID")
	} else if userId == "Yclid" {
		uploadURL, err = url.JoinPath("https://api-metrica.yandex.net/management/v1/counter/", counterId, "/offline_conversions/upload?client_id_type=YCLID")
	} else if userId == "UserId" {
		uploadURL, err = url.JoinPath("https://api-metrica.yandex.net/management/v1/counter/", counterId, "/offline_conversions/upload?client_id_type=USER_ID")
	} else {
		return common.AsyncUploadOutput{
			FailedReason:  "BRT: Failed to prepare upload url " + err.Error(),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}
	// println("Upload URL: ", uploadURL)
	// println("filepath: ", csvFilePath)

	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  "BRT: JSON Marshal Failed" + err.Error(),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	uploadTimeStat := stats.Default.NewTaggedStat("async_upload_time", stats.TimerType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
	})

	payloadSizeStat := stats.Default.NewTaggedStat("payload_size", stats.HistogramType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
	})

	startTime := time.Now()
	payloadSizeStat.Observe(float64(len(ympayload)))
	ym.logger.Debugf("[Async Destination Manager] File Upload Started for Dest Type %v", destType)

	req, err := http.NewRequest("POST", uploadURL, bytes.NewBuffer(ympayload))
	if err != nil {
		// TODO: handle this condition
	}
	resp, err := ym.client.Do(req) // We did this
	if err != nil {
		// TODO: handle this condition
	}
	var bodyBytes []byte
	if err == nil {
		// If no err returned by client.Post, reading body.
		// If reading body fails, retrying.
		bodyBytes, err = io.ReadAll(resp.Body)
	}
	var transResp oauthv2.TransportResponse
	// We don't need to handle it, as we can receive a string response even before executing OAuth operations like Refresh Token or Auth Status Toggle.
	// It's acceptable if the structure of respData doesn't match the oauthv2.TransportResponse struct.
	err = json.Unmarshal(bodyBytes, &transResp)
	if err == nil && transResp.OriginalResponse != "" {
		bodyBytes = []byte(transResp.OriginalResponse) // re-assign originalResponse
	}
	// ymresponseBody, statusCodeHTTP := misc.HTTPCallWithRetryWithTimeout(uploadURL, ympayload, config.GetDuration("HttpClient.yandexmetricaofflineevents.timeout", 10, time.Minute))
	ym.logger.Debugf("[Async Destination Manager] File Upload Finished for Dest Type %v", destType)
	uploadTimeStat.Since(startTime)

	if resp.StatusCode != http.StatusOK { // error scenario
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  fmt.Sprintf(`HTTP Call to Transformer Returned Non 200. StatusCode: %d`, resp.StatusCode),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	return common.AsyncUploadOutput{
		SuccessResponse: string(bodyBytes),
		DestinationID:   destination.ID,
	}
}

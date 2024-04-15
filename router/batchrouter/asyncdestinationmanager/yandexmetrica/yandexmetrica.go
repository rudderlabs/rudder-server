package yandexmetrica

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/yandexmetrica/augmenter"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	oauthv2common "github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	cntx "github.com/rudderlabs/rudder-server/services/oauth/v2/context"
	oauthv2httpclient "github.com/rudderlabs/rudder-server/services/oauth/v2/http"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/gjson"
)

var (
	json        = jsoniter.ConfigCompatibleWithStandardLibrary
	idClientMap = map[string]string{
		"ClientId": "CLIENT_ID",
		"Yclid":    "YCLID",
		"UserId":   "USER_ID",
	}
)

type YandexMetricaMessageBody struct {
	ClientID any     `json:"ClientId"`
	YclID    any     `json:"Yclid"`
	UserID   any     `json:"UserId"`
	Target   string  `json:"Target"`
	DateTime string  `json:"DateTime"`
	Price    float64 `json:"Price"`
	Currency string  `json:"Currency"`
}

type YandexMetricaMessage struct {
	Message YandexMetricaMessageBody `json:"message"`
}

type IdStruct struct {
	id         string
	clientType string
	headerName string
}

func (ym YandexMetricaMessageBody) Id() (IdStruct, error) {
	switch {
	case ym.ClientID != nil:
		_, ok := ym.ClientID.(string)
		if !ok {
			return IdStruct{}, fmt.Errorf("Non-string data for ClientID is not supported")
		}
		return IdStruct{id: ym.ClientID.(string), clientType: idClientMap["ClientId"], headerName: "ClientId"}, nil
	case ym.YclID != nil:
		_, ok := ym.YclID.(string)
		if !ok {
			return IdStruct{}, fmt.Errorf("Non-string data for Yclid is not supported")
		}
		return IdStruct{id: ym.YclID.(string), clientType: idClientMap["Yclid"], headerName: "Yclid"}, nil
	case ym.UserID != nil:
		_, ok := ym.UserID.(string)
		if !ok {
			return IdStruct{}, fmt.Errorf("Non-string data for UserId is not supported")
		}
		return IdStruct{id: ym.UserID.(string), clientType: idClientMap["UserId"], headerName: "UserId"}, nil
	default:
		return IdStruct{}, fmt.Errorf("...")
	}
}

const bufferSize = 5000 * 1024

type YandexMetricaBulkUploader struct {
	pollUrl         string
	logger          logger.Logger
	Client          *http.Client
	destinationInfo *oauthv2.DestinationInfo
}

func NewManager(destination *backendconfig.DestinationT, backendConfig backendconfig.BackendConfig) (*YandexMetricaBulkUploader, error) {
	destinationInfo := &oauthv2.DestinationInfo{
		Config:           destination.Config,
		DefinitionConfig: destination.DestinationDefinition.Config,
		WorkspaceID:      destination.WorkspaceID,
		DefinitionName:   destination.DestinationDefinition.Name,
		ID:               destination.ID,
	}
	yandexUploadManager := &YandexMetricaBulkUploader{
		destinationInfo: destinationInfo,
		logger:          logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("YandexMetrica").Child("YandexMetricaBulkUploader"),
	}
	cache := oauthv2.NewCache()
	optionalArgs := &oauthv2httpclient.HttpClientOptionalArgs{
		Logger:    yandexUploadManager.logger,
		Augmenter: augmenter.YandexReqAugmenter,
	}
	// TODO: Add http related timeout values
	originalHttpClient := &http.Client{Transport: &http.Transport{}}
	// This client is used for uploading data to yandex metrica
	yandexUploadManager.Client = oauthv2httpclient.NewOAuthHttpClient(
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
	return common.PollStatusResponse{}
}

func (ym *YandexMetricaBulkUploader) GetUploadStats(UploadStatsInput common.GetUploadStatsInput) common.GetUploadStatsResponse {
	return common.GetUploadStatsResponse{}
}

func generateCSVFromJSON(jsonData []byte, goalId string) (string, string, error) {
	// Define an empty map to store the parsed JSON data
	var data map[string]interface{}

	// Unmarshal the JSON data into
	var ymMsgs []YandexMetricaMessage
	err := json.Unmarshal(jsonData, &data)
	if err != nil {
		return "", "", err
	}
	inputData := gjson.GetBytes(jsonData, "input").String()
	err = json.Unmarshal([]byte(inputData), &ymMsgs)
	if err != nil {
		return "", "", err
	}

	ymMsgsBody := lo.Map(ymMsgs, func(ym YandexMetricaMessage, _ int) YandexMetricaMessageBody {
		return ym.Message
	})

	// Open the CSV file for writing
	localTmpDirName := fmt.Sprintf(`/%s/`, misc.RudderAsyncDestinationLogs)
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return "", "", err
	}
	folderPath := path.Join(tmpDirPath, localTmpDirName)
	_, e := os.Stat(folderPath)
	if os.IsNotExist(e) {
		folderPath, _ = os.MkdirTemp(folderPath, "")
	}
	path := path.Join(folderPath, uuid.NewString())
	csvFilePath := fmt.Sprintf(`%s.csv`, path)
	csvFile, err := os.Create(csvFilePath)
	_, _ = csvFile.Seek(0, 0)
	scanner := bufio.NewScanner(csvFile)
	scanner.Buffer(nil, bufferSize)

	if err != nil {
		return "", "", err
	}
	defer csvFile.Close()

	// Create a CSV writer
	csvWriter := csv.NewWriter(csvFile)

	// Define the header row based on key presence in "message" object
	firstMsgIdDetails, err := ymMsgsBody[0].Id()
	if err != nil {
		return "", "", fmt.Errorf("missing 'ClientId', 'Yclid', or 'UserId' key in 'message' object")
	}
	idDecider := firstMsgIdDetails.headerName
	header := []string{idDecider, "Target", "DateTime", "Price", "Currency"}
	// Write the header row
	err = csvWriter.Write(header)
	if err != nil {
		return "", "", err
	}

	// Extract and write data rows
	for _, ymMsg := range ymMsgsBody {
		if ymMsg.Target == "" {
			ymMsg.Target = goalId
		}

		idDetails, err := ymMsg.Id()
		if err != nil {
			continue
		}
		row := []string{
			fmt.Sprintf("%v", idDetails.id),
			fmt.Sprintf("%v", ymMsg.Target),
			fmt.Sprintf("%v", ymMsg.DateTime),
			fmt.Sprintf("%v", ymMsg.Price),
			fmt.Sprintf("%v", ymMsg.Currency),
		}
		err = csvWriter.Write(row)
		if err != nil {
			return "", "", err
		}
	}

	// Flush the writer
	csvWriter.Flush()

	// Return the chosen header
	return idDecider, csvFilePath, nil
}

func (ym *YandexMetricaBulkUploader) uploadFileToDestination(uploadURL, csvFilePath, userIdType string) (*http.Response, error) {
	payload := &bytes.Buffer{}
	writer := multipart.NewWriter(payload)
	file, openFileErr := os.Open(csvFilePath)
	if openFileErr != nil {
		return nil, openFileErr
	}
	defer file.Close()
	part1, createFormFileErr := writer.CreateFormFile("file", filepath.Base(csvFilePath))
	if createFormFileErr != nil {
		return nil, createFormFileErr
	}
	_, copyFileErr := io.Copy(part1, file)
	if copyFileErr != nil {
		return nil, copyFileErr
	}
	closeWriterErr := writer.Close()
	if closeWriterErr != nil {
		return nil, closeWriterErr
	}

	req, err := http.NewRequest(http.MethodPost, uploadURL, payload)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(cntx.CtxWithDestInfo(req.Context(), ym.destinationInfo))
	q := req.URL.Query()

	clientType, ok := idClientMap[userIdType]
	if !ok {
		return nil, fmt.Errorf("not a valid userId type")
	}
	q.Add("client_id_type", clientType)
	req.URL.RawQuery = q.Encode()
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, clientDoErr := ym.Client.Do(req)
	if clientDoErr != nil {
		return nil, clientDoErr
	}
	return resp, nil
}

func (ym *YandexMetricaBulkUploader) generateErrorOutput(errorString string, err error, importingJobIds []int64) common.AsyncUploadOutput {
	eventsAbortedStat := stats.Default.NewTaggedStat("failed_job_count", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": ym.destinationInfo.DefinitionName,
	})
	eventsAbortedStat.Count(len(importingJobIds))
	return common.AsyncUploadOutput{
		FailedCount:   len(importingJobIds),
		DestinationID: ym.destinationInfo.ID,
		FailedJobIDs:  importingJobIds,
		FailedReason:  fmt.Sprintf("%s %v", errorString, err.Error()),
	}
}

func (ym *YandexMetricaBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	startTime := time.Now()
	destination := asyncDestStruct.Destination
	filePath := asyncDestStruct.FileName
	destConfig, err := json.Marshal(destination.Config)
	if err != nil {
		return ym.generateErrorOutput("Error while marshalling destination config. ", err, asyncDestStruct.ImportingJobIDs)
	}
	destConfigJson := string(destConfig)
	// extract counterId from destConfigJson as a string value
	counterId := gjson.Get(destConfigJson, "counterId").String()
	goalId := gjson.Get(destConfigJson, "goalId").String()
	destType := destination.DestinationDefinition.Name
	importingJobIDs := asyncDestStruct.ImportingJobIDs
	file, err := os.Open(filePath)
	if err != nil {
		return ym.generateErrorOutput("Error while opening file. ", err, importingJobIDs)
	}
	defer file.Close()
	var input []common.AsyncJob
	decoder := json.NewDecoder(file)

	for decoder.More() {
		var tempJob common.AsyncJob
		err := decoder.Decode(&tempJob)
		if err != nil {
			return ym.generateErrorOutput("Error in Unmarshalling Job for Yandex Metrica destination:", err, importingJobIDs)
		}
		input = append(input, tempJob)
	}
	ympayload, err := json.Marshal(common.AsyncUploadT{
		Input:    input,
		Config:   destination.Config,
		DestType: strings.ToLower(destType),
	})
	if err != nil {
		return ym.generateErrorOutput("Error while marshalling AsyncUploadT.", err, importingJobIDs)
	}

	userIdType, csvFilePath, err := generateCSVFromJSON(ympayload, goalId)
	defer os.Remove(csvFilePath)
	if err != nil {
		return ym.generateErrorOutput("Error while generating CSV from JSON.", err, importingJobIDs)
	}

	uploadURL, err := url.JoinPath("https://api-metrica.yandex.net/management/v1/counter/", counterId, "/offline_conversions/upload")

	if err != nil {
		return ym.generateErrorOutput("Error while joining uploadUrl with counterId", err, importingJobIDs)
	}

	uploadTimeStat := stats.Default.NewTaggedStat("async_upload_time", stats.TimerType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
	})

	payloadSizeStat := stats.Default.NewTaggedStat("payload_size", stats.HistogramType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
	})

	eventsSuccessStat := stats.Default.NewTaggedStat("success_job_count", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
	})

	payloadSizeStat.Observe(float64(len(ympayload)))
	ym.logger.Debugf("[Async Destination Manager] File Upload Started for Dest Type %v", destType)

	resp, err := ym.uploadFileToDestination(uploadURL, csvFilePath, userIdType)
	if err != nil {
		return ym.generateErrorOutput("Error while uploading file to destination. ", err, importingJobIDs)
	}
	var bodyBytes []byte
	if err == nil {
		bodyBytes, err = io.ReadAll(resp.Body)
		if err != nil {
			return ym.generateErrorOutput("Error while reading response body. ", err, importingJobIDs)
		}
	}
	var transResp oauthv2.TransportResponse
	// We don't need to handle it, as we can receive a string response even before executing OAuth operations like Refresh Token or Auth Status Toggle.
	// It's acceptable if the structure of respData doesn't match the oauthv2.TransportResponse struct.
	err = json.Unmarshal(bodyBytes, &transResp)
	if err == nil && transResp.OriginalResponse != "" {
		bodyBytes = []byte(transResp.OriginalResponse) // re-assign originalResponse
	}
	ym.logger.Debugf("[Async Destination Manager] File Upload Finished for Dest Type %v", destType)
	uploadTimeStat.Since(startTime)

	if resp.StatusCode != http.StatusOK { // error scenario
		return ym.generateErrorOutput("got not 200 response from the destination", fmt.Errorf(string(bodyBytes)), importingJobIDs)
	}
	eventsSuccessStat.Count(len(asyncDestStruct.ImportingJobIDs))
	return common.AsyncUploadOutput{
		SucceededJobIDs: asyncDestStruct.ImportingJobIDs,
		SuccessResponse: string(bodyBytes),
		DestinationID:   ym.destinationInfo.ID,
	}
}

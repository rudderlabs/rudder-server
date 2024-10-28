package yandexmetrica

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/yandexmetrica/augmenter"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	oauthv2common "github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	cntx "github.com/rudderlabs/rudder-server/services/oauth/v2/context"
	oauthv2httpclient "github.com/rudderlabs/rudder-server/services/oauth/v2/http"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	json        = jsoniter.ConfigCompatibleWithStandardLibrary
	idClientMap = map[string]string{
		"ClientId": "CLIENT_ID",
		"Yclid":    "YCLID",
		"UserId":   "USER_ID",
	}
)

type yandexMetricaMessageBody struct {
	ClientID any     `json:"ClientId"`
	YclID    any     `json:"Yclid"`
	UserID   any     `json:"UserId"`
	Target   string  `json:"Target"`
	DateTime string  `json:"DateTime"`
	Price    float64 `json:"Price"`
	Currency string  `json:"Currency"`
}

type yandexMetricaMessage struct {
	Message yandexMetricaMessageBody `json:"message"`
}

type idStruct struct {
	id         string
	clientType string
	headerName string
}

func (ym yandexMetricaMessageBody) ID() (idStruct, error) {
	switch {
	case ym.ClientID != nil:
		return getID(ym.ClientID, "ClientId")
	case ym.YclID != nil:
		return getID(ym.YclID, "Yclid")
	case ym.UserID != nil:
		return getID(ym.UserID, "UserId")
	default:
		return idStruct{}, fmt.Errorf("no valid id found in message object")
	}
}

func getID(id interface{}, headerName string) (idStruct, error) {
	idString, ok := id.(string)
	if !ok {
		return idStruct{}, fmt.Errorf("non-string data for %s is not supported", headerName)
	}
	return idStruct{id: idString, clientType: idClientMap[headerName], headerName: headerName}, nil
}

type YandexMetricaBulkUploader struct {
	logger          logger.Logger
	statsFactory    stats.Stats
	Client          *http.Client
	destinationInfo *oauthv2.DestinationInfo
}

func NewManager(logger logger.Logger, statsFactory stats.Stats, destination *backendconfig.DestinationT, backendConfig backendconfig.BackendConfig) (*YandexMetricaBulkUploader, error) {
	destinationInfo := &oauthv2.DestinationInfo{
		Config:           destination.Config,
		DefinitionConfig: destination.DestinationDefinition.Config,
		WorkspaceID:      destination.WorkspaceID,
		DefinitionName:   destination.DestinationDefinition.Name,
		ID:               destination.ID,
	}
	yandexUploadManager := &YandexMetricaBulkUploader{
		destinationInfo: destinationInfo,
		logger:          logger.Child("YandexMetrica").Child("YandexMetricaBulkUploader"),
		statsFactory:    statsFactory,
	}
	cache := oauthv2.NewCache()
	optionalArgs := &oauthv2httpclient.HttpClientOptionalArgs{
		Logger:    yandexUploadManager.logger,
		Augmenter: augmenter.YandexReqAugmenter,
	}
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

// Poll return a success response for the poll request every time by default
func (ym *YandexMetricaBulkUploader) Poll(_ common.AsyncPoll) common.PollStatusResponse {
	return common.PollStatusResponse{}
}

// GetUploadStats return a success response for the getUploadStats request every time by default
func (ym *YandexMetricaBulkUploader) GetUploadStats(_ common.GetUploadStatsInput) common.GetUploadStatsResponse {
	return common.GetUploadStatsResponse{}
}

func generateCSVFromJSON(jsonData []byte, goalId string) (string, string, error) {
	// Define an empty map to store the parsed JSON data
	var ymMsgs []yandexMetricaMessage
	inputData := gjson.GetBytes(jsonData, "input").String()
	err := json.Unmarshal([]byte(inputData), &ymMsgs)
	if err != nil {
		return "", "", fmt.Errorf("unmarshalling transformed response: %v", err)
	}

	ymMsgsBody := lo.Map(ymMsgs, func(ym yandexMetricaMessage, _ int) yandexMetricaMessageBody {
		return ym.Message
	})

	// Open the CSV file for writing
	localTmpDirName := fmt.Sprintf(`/%s/`, misc.RudderAsyncDestinationLogs)
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return "", "", fmt.Errorf("creating tmp dir: %v", err)
	}
	folderPath := path.Join(tmpDirPath, localTmpDirName)
	_, err = os.Stat(folderPath)
	if os.IsNotExist(err) {
		folderPath, _ = os.MkdirTemp(folderPath, "")
	}
	csvPath := path.Join(folderPath, uuid.NewString())
	csvFilePath := fmt.Sprintf(`%s.csv`, csvPath)
	csvFile, err := os.Create(csvFilePath)
	if err != nil {
		return "", "", fmt.Errorf("creating csv file: %v", err)
	}

	defer func() { _ = csvFile.Close() }()

	// Create a CSV writer
	csvWriter := csv.NewWriter(csvFile)

	// Define the header row based on key presence in "message" object
	firstMsgIdDetails, err := ymMsgsBody[0].ID()
	if err != nil {
		return "", "", fmt.Errorf("missing 'ClientId', 'Yclid', or 'UserId' key in 'message' object")
	}
	idDecider := firstMsgIdDetails.headerName
	header := []string{idDecider, "Target", "DateTime", "Price", "Currency"}
	// Write the header row
	err = csvWriter.Write(header)
	if err != nil {
		return "", "", fmt.Errorf("writing header row: %v", err)
	}

	// Extract and write data rows
	for index, ymMsg := range ymMsgsBody {
		if ymMsg.Target == "" {
			ymMsg.Target = goalId
		}

		idDetails, err := ymMsg.ID()
		if err != nil {
			continue
		}
		err = csvWriter.Write([]string{
			idDetails.id,
			ymMsg.Target,
			ymMsg.DateTime,
			strconv.FormatFloat(ymMsg.Price, 'f', -1, 64),
			ymMsg.Currency,
		})
		if err != nil {
			return "", "", fmt.Errorf("writing data row: %v, index: %d", err, index)
		}
	}

	// Flush the writer
	csvWriter.Flush()

	// Return the chosen header
	return idDecider, csvFilePath, nil
}

func copyDataIntoBuffer(csvFilePath string) (*bytes.Buffer, *multipart.Writer, error) {
	payload := &bytes.Buffer{}
	writer := multipart.NewWriter(payload)
	file, openFileErr := os.Open(csvFilePath)
	if openFileErr != nil {
		return nil, nil, openFileErr
	}
	defer func() { _ = file.Close() }()
	part, createFormFileErr := writer.CreateFormFile("file", filepath.Base(csvFilePath))
	if createFormFileErr != nil {
		return nil, nil, createFormFileErr
	}
	_, copyFileErr := io.Copy(part, file)
	if copyFileErr != nil {
		return nil, nil, copyFileErr
	}
	closeWriterErr := writer.Close()
	if closeWriterErr != nil {
		return nil, nil, closeWriterErr
	}

	return payload, writer, nil
}

func (ym *YandexMetricaBulkUploader) uploadFileToDestination(uploadURL, csvFilePath, userIdType string) (*http.Response, error) {
	payload, writer, err := copyDataIntoBuffer(csvFilePath)
	if err != nil {
		return nil, fmt.Errorf("error while copying data into buffer: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, uploadURL, payload)
	if err != nil {
		return nil, fmt.Errorf("creating request: %v", err)
	}
	req = req.WithContext(cntx.CtxWithDestInfo(req.Context(), ym.destinationInfo))

	clientType, ok := idClientMap[userIdType]
	if !ok {
		return nil, fmt.Errorf("not a valid userId type")
	}
	q := req.URL.Query()
	q.Add("client_id_type", clientType)
	req.URL.RawQuery = q.Encode()
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := ym.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %v", err)
	}
	return resp, nil
}

func (ym *YandexMetricaBulkUploader) generateErrorOutput(errorString string, err error, importingJobIds []int64) common.AsyncUploadOutput {
	eventsAbortedStat := ym.statsFactory.NewTaggedStat("failed_job_count", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": ym.destinationInfo.DefinitionName,
	})
	eventsAbortedStat.Count(len(importingJobIds))
	return common.AsyncUploadOutput{
		AbortCount:    len(importingJobIds),
		DestinationID: ym.destinationInfo.ID,
		AbortJobIDs:   importingJobIds,
		AbortReason:   fmt.Sprintf("%s %v", errorString, err.Error()),
	}
}

func (*YandexMetricaBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(gjson.GetBytes(job.EventPayload, "body.JSON").String(), job.JobID)
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
		return ym.generateErrorOutput("opening file:", err, importingJobIDs)
	}
	defer file.Close()
	var input []common.AsyncJob
	decoder := json.NewDecoder(file)

	for decoder.More() {
		var tempJob common.AsyncJob
		err := decoder.Decode(&tempJob)
		if err != nil {
			return ym.generateErrorOutput("unmarshalling Job for Yandex Metrica destination:", err, importingJobIDs)
		}
		input = append(input, tempJob)
	}
	ympayload, err := json.Marshal(common.AsyncUploadT{
		Input:    input,
		Config:   destination.Config,
		DestType: strings.ToLower(destType),
	})
	if err != nil {
		return ym.generateErrorOutput("marshalling AsyncUploadT:", err, importingJobIDs)
	}
	statLabels := stats.Tags{
		"module":   "batch_router",
		"destType": destType,
	}

	userIdType, csvFilePath, err := generateCSVFromJSON(ympayload, goalId)
	defer os.Remove(csvFilePath)
	if err != nil {
		return ym.generateErrorOutput("generating CSV from JSON:", err, importingJobIDs)
	}

	uploadURL, err := url.JoinPath("https://api-metrica.yandex.net/management/v1/counter/", counterId, "/offline_conversions/upload")
	if err != nil {
		return ym.generateErrorOutput("joining uploadUrl with counterId", err, importingJobIDs)
	}

	uploadTimeStat := ym.statsFactory.NewTaggedStat("async_upload_time", stats.TimerType, statLabels)

	payloadSizeStat := ym.statsFactory.NewTaggedStat("payload_size", stats.HistogramType, statLabels)

	eventsSuccessStat := ym.statsFactory.NewTaggedStat("success_job_count", stats.CountType, statLabels)

	payloadSizeStat.Observe(float64(len(ympayload)))
	ym.logger.Debugf("[Async Destination Manager] File Upload Started for Dest Type %v\n", destType)

	resp, err := ym.uploadFileToDestination(uploadURL, csvFilePath, userIdType)
	if err != nil {
		return ym.generateErrorOutput("uploading file to destination. ", err, importingJobIDs)
	}
	var bodyBytes []byte

	bodyBytes, err = io.ReadAll(resp.Body)
	defer func() { _ = resp.Body.Close() }()
	if err != nil {
		return ym.generateErrorOutput("reading response body. ", err, importingJobIDs)
	}

	var transResp oauthv2.TransportResponse
	// We don't need to handle it, as we can receive a string response even before executing OAuth operations like Refresh Token or Auth Status Toggle.
	// It's acceptable if the structure of respData doesn't match the oauthv2.TransportResponse struct.
	err = json.Unmarshal(bodyBytes, &transResp)
	if err == nil && transResp.OriginalResponse != "" {
		bodyBytes = []byte(transResp.OriginalResponse) // re-assign originalResponse
	}
	ym.logger.Debugf("[Async Destination Manager] File Upload Finished for Dest Type %v\n", destType)
	uploadTimeStat.Since(startTime)

	if resp.StatusCode != http.StatusOK { // error scenario
		return ym.generateErrorOutput("got non 200 response from the destination", errors.New(string(bodyBytes)), importingJobIDs)
	}
	eventsSuccessStat.Count(len(asyncDestStruct.ImportingJobIDs))
	return common.AsyncUploadOutput{
		SucceededJobIDs: asyncDestStruct.ImportingJobIDs,
		SuccessResponse: string(bodyBytes),
		DestinationID:   ym.destinationInfo.ID,
	}
}

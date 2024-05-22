package klaviyobulkupload

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

var (
	json = jsoniter.ConfigCompatibleWithStandardLibrary
)

type KlaviyoBulkUploader struct {
	destName          string
	destinationConfig map[string]interface{}
	logger            logger.Logger
	Client            *http.Client
}

type UploadResp struct {
	Data struct {
		Id string `json:"id"`
	} `json:"data"`
}

type PollResp struct {
	Data struct {
		Id         string `json:"id"`
		Attributes struct {
			Total_count     int    `json:"total_count"`
			Completed_count int    `json:"completed_count"`
			Failed_count    int    `json:"failed_count"`
			Status          string `json:"status"`
		} `json:"attributes"`
	} `json:"data"`
}

func NewManager(destination *backendconfig.DestinationT) (*KlaviyoBulkUploader, error) {
	return &KlaviyoBulkUploader{
		destName:          destination.DestinationDefinition.Name,
		destinationConfig: destination.Config,
		logger:            logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("KlaviyoBulkUpload").Child("KlaviyoBulkUploader"),
	}, nil
}

func (kbu *KlaviyoBulkUploader) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
	client := &http.Client{}
	destConfig := kbu.destinationConfig
	privateApiKey, _ := destConfig["privateApiKey"].(string)
	importId := pollInput.ImportId
	// pp.Println("ImportId: ", importId)
	// pp.Println("PrivateApiKey: ", privateApiKey)
	pollUrl := "https://a.klaviyo.com/api/profile-bulk-import-jobs/" + importId
	req, err := http.NewRequest("GET", pollUrl, nil)
	if err != nil {
		// pp.Println("inside error")
		return common.PollStatusResponse{
			Complete:   false,
			InProgress: false,
			HasFailed:  true,
			Error:      err.Error(),
		}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization",
		"Klaviyo-API-Key "+privateApiKey)
	req.Header.Set("revision", "2024-05-15")
	resp, err := client.Do(req)
	if err != nil {
		return common.PollStatusResponse{
			Complete:   false,
			InProgress: false,
			StatusCode: 0,
			HasFailed:  true,
			Error:      err.Error(),
			HasWarning: false,
		}
	}

	var pollBodyBytes []byte
	var pollresp PollResp
	pollBodyBytes, _ = io.ReadAll(resp.Body)
	defer func() { _ = resp.Body.Close() }()

	pollRespErr := json.Unmarshal(pollBodyBytes, &pollresp)
	if pollRespErr != nil {
		return common.PollStatusResponse{
			Complete:   false,
			InProgress: false,
			StatusCode: 0,
			HasFailed:  true,
			Error:      pollRespErr.Error(),
			HasWarning: false,
		}
	}
	// totalCount := uploadresp.data.attributes.total_count
	// completedCount := uploadresp.data.attributes.completed_count
	// pp.Println("Attributes: ", pollresp.Data.Attributes)
	failCount := pollresp.Data.Attributes.Failed_count
	status := pollresp.Data.Attributes.Status
	// fmt.Println("Status: ", status)
	// fmt.Println("FailCount: ", failCount)

	defer func() { _ = resp.Body.Close() }()

	switch status {
	case "queued", "processing":
		// pp.Print("inside processing")
		return common.PollStatusResponse{
			Complete:   false,
			InProgress: true,
		}
	case "complete":
		// pp.Print("inside complete")
		if failCount > 0 {
			return common.PollStatusResponse{
				Complete:      true,
				InProgress:    false,
				StatusCode:    200,
				HasFailed:     true,
				HasWarning:    false,
				FailedJobURLs: importId,
			}
		} else {
			// pp.Print("inside success")
			return common.PollStatusResponse{
				Complete:   true,
				InProgress: false,
				StatusCode: 200,
				HasFailed:  false,
				HasWarning: false,
			}
		}
	default:
		// pp.Print("inside default")
		return common.PollStatusResponse{
			Complete:   false,
			StatusCode: 500,
			InProgress: true,
		}
	}
}

func (kbu *KlaviyoBulkUploader) GetUploadStats(UploadStatsInput common.GetUploadStatsInput) common.GetUploadStatsResponse {
	// destConfig := kbu.destinationConfig
	// privateApiKey, _ := destConfig["privateApiKey"].(string)
	// importId := UploadStatsInput.ImportingList
	// pp.Println(privateApiKey)
	// pp.Println(importId)

	// var uploadStatusResponse common.GetUploadStatsResponse
	return common.GetUploadStatsResponse{}
}
func (kbu *KlaviyoBulkUploader) generateKlaviyoErrorOutput(errorString string, err error, importingJobIds []int64, destinationID string) common.AsyncUploadOutput {
	eventsAbortedStat := stats.Default.NewTaggedStat("failed_job_count", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": "KLAVIYO_BULK_UPLOAD",
	})
	eventsAbortedStat.Count(len(importingJobIds))
	// pp.Println("importingJobIds: ", importingJobIds, len(importingJobIds))
	return common.AsyncUploadOutput{
		AbortCount:    len(importingJobIds),
		DestinationID: destinationID,
		AbortJobIDs:   importingJobIds,
		AbortReason:   fmt.Sprintf("%s %v", errorString, err.Error()),
	}
}

func extractProfiles(input map[string]interface{}) ([]map[string]interface{}, error) {
	message, ok := input["message"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("message field not found or not a map")
	}

	data, ok := message["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("data field not found or not a map")
	}

	attributes, ok := data["attributes"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("attributes field not found or not a map")
	}

	profilesContainer, ok := attributes["profiles"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("profiles field not found or not a map")
	}

	profiles, ok := profilesContainer["data"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("data field in profiles not found or not a slice")
	}

	var profileMaps []map[string]interface{}
	for _, profile := range profiles {
		profileMap, ok := profile.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("profile is not a map")
		}
		profileMaps = append(profileMaps, profileMap)
	}

	return profileMaps, nil
}

func (kbu *KlaviyoBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	startTime := time.Now()
	destination := asyncDestStruct.Destination
	filePath := asyncDestStruct.FileName
	importingJobIDs := asyncDestStruct.ImportingJobIDs
	// pp.Println("Importing Job IDs: ", importingJobIDs)
	destType := destination.DestinationDefinition.Name
	destinationID := destination.ID
	// var failedJobs []int64
	// var successJobs []int64
	// var importIds []string
	// var errors []string
	listId, _ := destination.Config["listId"].(string)
	statLabels := stats.Tags{
		"module":   "batch_router",
		"destType": destType,
	}
	// destConfig, err := json.Marshal(destination.Config)
	// if err != nil {
	// 	return kbu.generateKlaviyoErrorOutput("Error while marshalling destination config. ", err, asyncDestStruct.ImportingJobIDs, destinationID)
	// }
	file, err := os.Open(filePath)
	if err != nil {
		return kbu.generateKlaviyoErrorOutput("Error while opening file. ", err, asyncDestStruct.ImportingJobIDs, destinationID)
	}
	defer file.Close()
	// var input []common.AsyncJob
	var combinedProfiles []map[string]interface{}
	// decoder := json.NewDecoder(file)

	// for {
	// 	var input map[string]interface{}
	// 	if err := decoder.Decode(&input); err != nil {
	// 		if err == io.EOF {
	// 			break
	// 		}
	// 		return kbu.generateKlaviyoErrorOutput("Error while parsing JSON.", err, importingJobIDs, destinationID)
	// 	}
	// 	profiles, err := extractProfiles(input)
	// 	if err != nil {
	// 		return kbu.generateKlaviyoErrorOutput("Error while extracting profiles.", err, importingJobIDs, destinationID)
	// 	}
	// 	combinedProfiles = append(combinedProfiles, profiles...)
	// }
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		var input map[string]interface{}
		line := scanner.Text()
		if err := json.Unmarshal([]byte(line), &input); err != nil {
			return kbu.generateKlaviyoErrorOutput("Error while parsing JSON.", err, importingJobIDs, destinationID)
		}
		profiles, err := extractProfiles(input)
		if err != nil {
			return kbu.generateKlaviyoErrorOutput("Error while extracting profiles.", err, importingJobIDs, destinationID)
		}
		combinedProfiles = append(combinedProfiles, profiles...)
	}

	if err := scanner.Err(); err != nil {
		return kbu.generateKlaviyoErrorOutput("Error while reading file.", err, importingJobIDs, destinationID)
	}

	// Create the combined payload
	// check if listId is present in the destination config
	var combinedPayload map[string]interface{}
	if listId == "" {
		combinedPayload = map[string]interface{}{
			"data": map[string]interface{}{
				"type": "profile-bulk-import-job",
				"attributes": map[string]interface{}{
					"data": map[string]interface{}{
						"profiles": combinedProfiles,
					},
				},
			},
		}
	} else {
		combinedPayload = map[string]interface{}{
			"data": map[string]interface{}{
				"type": "profile-bulk-import-job",
				"attributes": map[string]interface{}{
					"profiles": map[string]interface{}{
						"data": combinedProfiles,
					},
				},
				"relationships": map[string]interface{}{
					"lists": map[string]interface{}{
						"data": []map[string]interface{}{
							{
								"type": "list",
								"id":   listId,
							},
						},
					},
				},
			},
		}
	}

	// Convert combined payload to JSON
	outputJSON, err := json.MarshalIndent(combinedPayload, "", "  ")
	if err != nil {
		return kbu.generateKlaviyoErrorOutput("Error while marshaling combined JSON.", err, importingJobIDs, destinationID)
	}
	outputFilePath := "combined_payload.json"
	if err := os.WriteFile(outputFilePath, outputJSON, 0644); err != nil {
		return kbu.generateKlaviyoErrorOutput("Error while writing JSON to file.", err, importingJobIDs, destinationID)
	}
	// pp.Println(filePath)
	// destConfigJson := string(destConfig)
	// pp.Println(destConfigJson)
	// failedJobIDs := asyncDestStruct.FailedJobIDs
	uploadURL := "https://a.klaviyo.com/api/profile-bulk-import-jobs/"
	client := &http.Client{}
	req, err := http.NewRequest("POST", uploadURL, bytes.NewBuffer(outputJSON))
	if err != nil {
		return kbu.generateKlaviyoErrorOutput("Error while creating request.", err, importingJobIDs, destinationID)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Klaviyo-API-Key "+destination.Config["privateApiKey"].(string))
	req.Header.Set("revision", "2024-05-15")

	uploadTimeStat := stats.Default.NewTaggedStat("async_upload_time", stats.TimerType, statLabels)
	payloadSizeStat := stats.Default.NewTaggedStat("payload_size", stats.HistogramType, statLabels)
	eventsSuccessStat := stats.Default.NewTaggedStat("success_job_count", stats.CountType, statLabels)
	payloadSizeStat.Observe(float64(len(outputJSON)))

	resp, err := client.Do(req)
	if err != nil {
		return kbu.generateKlaviyoErrorOutput("Error while sending request.", err, importingJobIDs, destinationID)
	}

	var bodyBytes []byte
	bodyBytes, _ = io.ReadAll(resp.Body)
	defer func() { _ = resp.Body.Close() }()
	// pp.Println("Response Status:", resp.Status)
	// pp.Println("Response Body:", string(bodyBytes))
	uploadTimeStat.Since(startTime)

	if resp.StatusCode != 202 {
		return kbu.generateKlaviyoErrorOutput("Error while sending request.", fmt.Errorf(string(bodyBytes)), importingJobIDs, destinationID)
	}
	var parameters common.ImportParameters
	var uploadresp UploadResp
	uploadRespErr := json.Unmarshal([]byte(bodyBytes), &uploadresp)
	// pp.Println("Error: ", err)
	if uploadRespErr != nil {
		return kbu.generateKlaviyoErrorOutput("Error while unmarshaling response.", uploadRespErr, importingJobIDs, destinationID)
	}
	parameters.ImportId = uploadresp.Data.Id
	importParameters, err := json.Marshal(parameters)
	// println("Import Parameters: ", string(importParameters))
	if err != nil {
		return kbu.generateKlaviyoErrorOutput("Error while marshaling parameters.", err, importingJobIDs, destinationID)
	}
	eventsSuccessStat.Count(len(asyncDestStruct.ImportingJobIDs))
	return common.AsyncUploadOutput{
		ImportingParameters: importParameters,
		ImportingJobIDs:     importingJobIDs,
		SuccessResponse:     string(bodyBytes),
		DestinationID:       destination.ID,
	}
}

func (kbu *KlaviyoBulkUploader) GetErrorStats() map[string]interface{} {
	return nil
}

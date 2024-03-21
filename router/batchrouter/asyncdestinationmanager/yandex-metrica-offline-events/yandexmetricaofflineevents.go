package yandex_metrica_offline_events

// type YandexMetricaBulkUploader struct {
// 	destName          string
// 	destinationConfig map[string]interface{}
// 	transformUrl      string
// 	pollUrl           string
// 	logger            logger.Logger
// 	timeout           time.Duration
// }

// func NewManager(destination *backendconfig.DestinationT) (*YandexMetricaBulkUploader, error) {
// 	YandexMetricaBulkUpload := &YandexMetricaBulkUploader{
// 		destName:          destination.DestinationDefinition.Name,
// 		destinationConfig: destination.Config,
// 		pollUrl:           "/pollStatus",
// 		transformUrl:      config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090"),
// 		logger:            logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("YandexMetrica").Child("YandexMetricaBulkUploader"),
// 		timeout:           config.GetDuration("HttpClient.yandexMetricaBulkUpload.timeout", 30, time.Second),
// 	}
// 	return YandexMetricaBulkUpload, nil
// }

// func (b *YandexMetricaBulkUploader) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
// 	finalPollInput := marketoPollInputStruct{
// 		ImportId:   pollInput.ImportId,
// 		DestType:   "MARKETO_BULK_UPLOAD",
// 		DestConfig: b.destinationConfig,
// 	}
// 	payload, err := json.Marshal(finalPollInput)
// 	if err != nil {
// 		b.logger.Errorf("Error in Marshalling Poll Input: %v", err)
// 		return common.PollStatusResponse{
// 			StatusCode: 500,
// 			HasFailed:  true,
// 		}
// 	}

// 	pollURL, err := url.JoinPath(b.transformUrl, b.pollUrl)
// 	if err != nil {
// 		b.logger.Errorf("Error in preparing poll url: %v", err)
// 		return common.PollStatusResponse{
// 			StatusCode: 500,
// 			HasFailed:  true,
// 			Error:      err.Error(),
// 		}
// 	}

// 	bodyBytes, transformerConnectionStatus := misc.HTTPCallWithRetryWithTimeout(pollURL, payload, b.timeout)
// 	if transformerConnectionStatus != 200 {
// 		return common.PollStatusResponse{
// 			StatusCode: transformerConnectionStatus,
// 			HasFailed:  true,
// 		}
// 	}
// 	var asyncResponse common.PollStatusResponse
// 	err = json.Unmarshal(bodyBytes, &asyncResponse)
// 	if err != nil {
// 		// needs to be retried
// 		b.logger.Errorf("Error in Unmarshalling Poll Response: %v", err)
// 		return common.PollStatusResponse{
// 			StatusCode: 500,
// 			HasFailed:  true,
// 		}
// 	}

// 	if asyncResponse.Error != "" {
// 		b.logger.Errorw("[Batch Router] Failed to fetch status for",
// 			lf.DestinationType, "MARKETO_BULK_UPLOAD",
// 			"body", string(bodyBytes[:512]),
// 			lf.Error, asyncResponse.Error,
// 		)
// 		return common.PollStatusResponse{
// 			StatusCode: 500,
// 			HasFailed:  true,
// 			Error:      asyncResponse.Error,
// 		}
// 	}
// 	return asyncResponse
// }

// type YandexMetricaOfflineEvents struct {
// 	// Add your configuration fields here
// }

// func NewYandexMetricaOfflineEvents() *YandexMetricaOfflineEvents {
// 	// Initialize and return a new instance of YandexMetricaOfflineEvents
// 	return &YandexMetricaOfflineEvents{}
// }

// func (d *YandexMetricaOfflineEvents) SendEvent(event interface{}) error {
// 	// Implement the logic to send the event to Yandex Metrica offline events destination
// 	fmt.Println("Sending event to Yandex Metrica offline events:", event)
// 	return nil
// }

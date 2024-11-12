package klaviyobulkupload

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

const (
	KlaviyoAPIURL = "https://a.klaviyo.com/api/profile-bulk-import-jobs/"
)

type KlaviyoAPIServiceImpl struct {
	client        *http.Client
	PrivateAPIKey string
	logger        logger.Logger
	statsFactory  stats.Stats
	statLabels    stats.Tags
}

func setRequestHeaders(req *http.Request, apiKey string) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Klaviyo-API-Key "+apiKey)
	req.Header.Set("revision", "2024-05-15")
}

func (k *KlaviyoAPIServiceImpl) UploadProfiles(profiles Payload) (*UploadResp, error) {
	payloadJSON, err := json.Marshal(profiles)
	payloadSizeStat := k.statsFactory.NewTaggedStat("payload_size", stats.HistogramType, k.statLabels)
	payloadSizeStat.Observe(float64(len(payloadJSON)))

	if err != nil {
		return nil, err
	}
	startTime := time.Now()
	req, err := http.NewRequest("POST", KlaviyoAPIURL, bytes.NewBuffer(payloadJSON))
	if err != nil {
		return nil, err
	}
	setRequestHeaders(req, k.PrivateAPIKey)
	resp, err := k.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusAccepted {
		return nil, fmt.Errorf("upload failed with status code %d", resp.StatusCode)
	}

	uploadTimeStat := k.statsFactory.NewTaggedStat("async_upload_time", stats.TimerType, k.statLabels)
	uploadTimeStat.Since(startTime)
	var uploadResp UploadResp
	uploadBodyBytes, _ := io.ReadAll(resp.Body)
	defer func() { _ = resp.Body.Close() }()
	uploadRespErr := json.Unmarshal(uploadBodyBytes, &uploadResp)
	if uploadRespErr != nil {
		return nil, uploadRespErr
	}
	return &uploadResp, uploadRespErr
}

func (k *KlaviyoAPIServiceImpl) GetUploadStatus(importId string) (*PollResp, error) {
	pollUrl := KlaviyoAPIURL + importId
	req, err := http.NewRequest("GET", pollUrl, nil)
	if err != nil {
		return nil, err
	}
	setRequestHeaders(req, k.PrivateAPIKey)
	resp, err := k.client.Do(req)
	if err != nil {
		return nil, err
	}
	var pollBodyBytes []byte
	var pollresp PollResp
	pollBodyBytes, _ = io.ReadAll(resp.Body)
	defer func() { _ = resp.Body.Close() }()

	pollRespErr := json.Unmarshal(pollBodyBytes, &pollresp)
	return &pollresp, pollRespErr
}

func (k *KlaviyoAPIServiceImpl) GetUploadErrors(importId string) (*UploadStatusResp, error) {
	importErrorUrl := KlaviyoAPIURL + importId + "/import-errors"
	req, err := http.NewRequest("GET", importErrorUrl, nil)
	if err != nil {
		return nil, err
	}
	setRequestHeaders(req, k.PrivateAPIKey)
	resp, err := k.client.Do(req)
	if err != nil {
		return nil, err
	}
	var importErrorBodyBytes []byte
	var importErrorResp UploadStatusResp
	importErrorBodyBytes, _ = io.ReadAll(resp.Body)
	defer func() { _ = resp.Body.Close() }()
	importErrorRespErr := json.Unmarshal(importErrorBodyBytes, &importErrorResp)
	return &importErrorResp, importErrorRespErr
}

func NewKlaviyoAPIService(destination *backendconfig.DestinationT, logger logger.Logger, statsFactory stats.Stats) KlaviyoAPIService {
	return &KlaviyoAPIServiceImpl{
		client:        http.DefaultClient,
		PrivateAPIKey: destination.Config["privateApiKey"].(string),
		logger:        logger,
		statsFactory:  statsFactory,
		statLabels: stats.Tags{
			"module":   "batch_router",
			"destType": destination.Name,
			"destID":   destination.ID,
		},
	}
}

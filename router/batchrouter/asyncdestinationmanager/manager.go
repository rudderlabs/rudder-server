package asyncdestinationmanager

import (
	"errors"

	jsoniter "github.com/json-iterator/go"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	bingads "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/eloqua"
	marketobulkupload "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/marketo-bulk-upload"
	yandexmetricaofflineevents "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/yandex-metrica-offline-events"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func GetMarshalledData(payload string, jobID int64) string {
	var job common.AsyncJob
	err := json.Unmarshal([]byte(payload), &job.Message)
	if err != nil {
		panic("Unmarshalling Transformer Response Failed")
	}
	job.Metadata = make(map[string]interface{})
	job.Metadata["job_id"] = jobID
	responsePayload, err := json.Marshal(job)
	if err != nil {
		panic("Marshalling Response Payload Failed")
	}
	return string(responsePayload)
}

func NewManager(destination *backendconfig.DestinationT, backendConfig backendconfig.BackendConfig) (common.AsyncDestinationManager, error) {
	switch destination.DestinationDefinition.Name {
	case "BINGADS_AUDIENCE":
		return bingads.NewManager(destination, backendConfig)
	case "MARKETO_BULK_UPLOAD":
		return marketobulkupload.NewManager(destination)
	case "ELOQUA":
		return eloqua.NewManager(destination)
	case "YANDEX_METRICA_OFFLINE_EVENTS":
		return yandexmetricaofflineevents.NewManager(destination)
	}
	return nil, errors.New("invalid destination type")
}

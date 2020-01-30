package diagnosis

import (
	"github.com/rudderlabs/analytics-go"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"time"
)

const (
	StartTime           = "diagnosis_start_time"
	InstanceId          = "server_instance_id"
	ServerStart         = "server_start"
	ConfigProcessed     = "config_processed"
	SourcesCount        = "no_of_sources"
	DesitanationCount   = "no_of_destinations"
	ServerStarted       = "server_started"
	ConfigIdentify      = "identify"
	GatewayEvents       = "gateway_events"
	GatewaySuccess      = "gateway_success"
	GatewayFailure      = "gateway_failure"
	RouterEvents        = "router_events"
	RouterType          = "router_type"
	RouterAborted       = "router_aborted"
	RouterRetries       = "router_retries"
	RouterSuccess       = "router_success"
	RouterCompletedTime = "router_average_job_time"
	BatchRouterEvents   = "batch_router_events"
	BatchRouterType     = "batch_router_type"
	BatchRouterSuccess  = "batch_router_success"
	BatchRouterFailed   = "batch_router_failed"
)

var (
	EnableDiagnosis             bool
	endpoint                    string
	writekey                    string
	EnableServerStartMetric     bool
	EnableConfigIdentifyMetric  bool
	EnableServerStartedMetric   bool
	EnableConfigProcessedMetric bool
	EnableGatewayMetric         bool
	EnableRouterMetric          bool
	EnableBatchRouterMetric     bool
)

var diagnosis Diagnosis

type Diagnosis struct {
	Client     analytics.Client
	StartTime  time.Time
	UniqueId   string
	InstanceId string
}

func init() {
	config.Initialize()
	loadConfig()
	diagnosis.InstanceId = config.GetEnv("INSTANCE_ID", "1")
	config := analytics.Config{
		Endpoint: endpoint,
	}
	client, _ := analytics.NewWithConfig(writekey, config)
	diagnosis.Client = client
	diagnosis.StartTime = time.Now()
	diagnosis.UniqueId = misc.GetHash(misc.GetMacAddress())
}
func loadConfig() {
	EnableDiagnosis = config.GetBool("Diagnosis.enableDiagnosis", true)
	endpoint = config.GetString("Diagnosis.endpoint", "")
	writekey = config.GetString("Diagnosis.writekey", "")
	EnableServerStartMetric = config.GetBool("Diagnosis.enableServerStartMetric", true)
	EnableConfigIdentifyMetric = config.GetBool("Diagnosis.enableConfigIdentifyMetric", true)
	EnableServerStartedMetric = config.GetBool("Diagnosis.enableServerStartedMetric", true)
	EnableConfigProcessedMetric = config.GetBool("Diagnosis.enableConfigProcessedMetric", true)
	EnableGatewayMetric = config.GetBool("Diagnosis.enableGatewayMetric", true)
	EnableRouterMetric = config.GetBool("Diagnosis.enableRouterMetric", true)
	EnableBatchRouterMetric = config.GetBool("Diagnosis.enableBatchRouterMetric", true)
}

func Track(event string, properties map[string]interface{}) {
	if EnableDiagnosis {
		properties[StartTime] = diagnosis.StartTime
		properties[InstanceId] = diagnosis.InstanceId
		diagnosis.Client.Enqueue(
			analytics.Track{
				Event:      event,
				Properties: properties,
				UserId:     diagnosis.UniqueId,
			},
		)
	}
}

func Identify(event string, properties map[string]interface{}) {
	if EnableDiagnosis {
		properties[StartTime] = diagnosis.StartTime
		properties[InstanceId] = diagnosis.InstanceId
		diagnosis.Client.Enqueue(
			analytics.Track{
				Event:      event,
				Properties: properties,
				UserId:     diagnosis.UniqueId,
			},
		)
	}
}

package diagnostics

import (
	"time"

	"github.com/rudderlabs/analytics-go"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
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
	RouterAborted       = "router_aborted"
	RouterRetries       = "router_retries"
	RouterSuccess       = "router_success"
	RouterCompletedTime = "router_average_job_time"
	BatchRouterEvents   = "batch_router_events"
	BatchRouterSuccess  = "batch_router_success"
	BatchRouterFailed   = "batch_router_failed"
)

var (
	EnableDiagnostics           bool
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

var diagnostics Diagnostics

type Diagnostics struct {
	Client     analytics.Client
	StartTime  time.Time
	UniqueId   string
	UserId     string
	InstanceId string
}

func init() {
	config.Initialize()
	loadConfig()
	diagnostics.InstanceId = config.GetEnv("INSTANCE_ID", "1")
	config := analytics.Config{
		Endpoint: endpoint,
	}
	client, _ := analytics.NewWithConfig(writekey, config)
	diagnostics.Client = client
	diagnostics.StartTime = time.Now()
	diagnostics.UniqueId = misc.GetMD5Hash(misc.GetMacAddress())
}

func loadConfig() {
	EnableDiagnostics = config.GetBool("Diagnosis.enableDiagnosis", true)
	endpoint = config.GetString("Diagnosis.endpoint", "https://hosted.rudderlabs.com")
	writekey = config.GetString("Diagnosis.writekey", "1Zn529y077Awo0fE7RiRrAVSgvs")
	EnableServerStartMetric = config.GetBool("Diagnosis.enableServerStartMetric", true)
	EnableConfigIdentifyMetric = config.GetBool("Diagnosis.enableConfigIdentifyMetric", true)
	EnableServerStartedMetric = config.GetBool("Diagnosis.enableServerStartedMetric", true)
	EnableConfigProcessedMetric = config.GetBool("Diagnosis.enableConfigProcessedMetric", true)
	EnableGatewayMetric = config.GetBool("Diagnosis.enableGatewayMetric", true)
	EnableRouterMetric = config.GetBool("Diagnosis.enableRouterMetric", true)
	EnableBatchRouterMetric = config.GetBool("Diagnosis.enableBatchRouterMetric", true)
}

func Track(event string, properties map[string]interface{}) {
	if EnableDiagnostics {
		properties[StartTime] = diagnostics.StartTime
		properties[InstanceId] = diagnostics.InstanceId
		diagnostics.Client.Enqueue(
			analytics.Track{
				Event:       event,
				Properties:  properties,
				AnonymousId: diagnostics.UniqueId,
				UserId:      diagnostics.UserId,
			},
		)
	}
}

func DisableMetrics(enableMetrics bool) {
	if !enableMetrics {
		EnableServerStartedMetric = false
		EnableConfigProcessedMetric = false
		EnableGatewayMetric = false
		EnableRouterMetric = false
		EnableBatchRouterMetric = false
	}

}

func Identify(properties map[string]interface{}) {
	if EnableDiagnostics {
		// add in traits
		if val, ok := properties[ConfigIdentify]; ok {
			diagnostics.UserId = val.(string)
		}
		diagnostics.Client.Enqueue(
			analytics.Identify{
				AnonymousId: diagnostics.UniqueId,
				UserId:      diagnostics.UserId,
			},
		)
	}
}

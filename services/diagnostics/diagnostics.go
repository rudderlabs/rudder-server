package diagnostics

import (
	"time"

	"github.com/rudderlabs/analytics-go"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	StartTime              = "diagnosis_start_time"
	InstanceId             = "server_instance_id"
	ServerStart            = "server_start"
	ConfigProcessed        = "config_processed"
	SourcesCount           = "no_of_sources"
	DesitanationCount      = "no_of_destinations"
	ServerStarted          = "server_started"
	ConfigIdentify         = "identify"
	GatewayEvents          = "gateway_events"
	GatewaySuccess         = "gateway_success"
	GatewayFailure         = "gateway_failure"
	RouterEvents           = "router_events"
	RouterAborted          = "router_aborted"
	RouterRetries          = "router_retries"
	RouterSuccess          = "router_success"
	RouterFailed           = "router_failed"
	RouterDestination      = "router_destination"
	RouterAttemptNum       = "router_attempt_num"
	RouterCompletedTime    = "router_average_job_time"
	BatchRouterEvents      = "batch_router_events"
	BatchRouterSuccess     = "batch_router_success"
	BatchRouterFailed      = "batch_router_failed"
	BatchRouterDestination = "batch_router_destination"
	UserID                 = "user_id"
	ErrorCode              = "error_code"
	ErrorResponse          = "error_response"
	Count                  = "count"
	ErrorCountMap          = "error_count_map"
)

var (
	EnableDiagnostics               bool
	EnableServerStartMetric         bool
	EnableConfigIdentifyMetric      bool
	EnableServerStartedMetric       bool
	EnableConfigProcessedMetric     bool
	EnableGatewayMetric             bool
	EnableRouterMetric              bool
	EnableBatchRouterMetric         bool
	EnableDestinationFailuresMetric bool

	endpoint string
	writekey string
)
var Diagnostics DiagnosticsI

type DiagnosticsI interface {
	Track(event string, properties map[string]interface{})
	DisableMetrics(enableMetrics bool)
	Identify(properties map[string]interface{})
}
type diagnostics struct {
	Client     analytics.Client
	StartTime  time.Time
	UniqueId   string
	UserId     string
	InstanceId string
}

func Init() {
	loadConfig()
}

func loadConfig() {
	config.RegisterBoolConfigVariable(true, &EnableDiagnostics, false, "Diagnostics.enableDiagnostics")
	config.RegisterStringConfigVariable("https://rudderstack-dataplane.rudderstack.com", &endpoint, false, "Diagnostics.endpoint")
	config.RegisterStringConfigVariable("1aWPBIROQvFYW9FHxgc03nUsLza", &writekey, false, "Diagnostics.writekey")
	config.RegisterBoolConfigVariable(true, &EnableServerStartMetric, false, "Diagnostics.enableServerStartMetric")
	config.RegisterBoolConfigVariable(true, &EnableConfigIdentifyMetric, false, "Diagnostics.enableConfigIdentifyMetric")
	config.RegisterBoolConfigVariable(true, &EnableServerStartedMetric, false, "Diagnostics.enableServerStartedMetric")
	config.RegisterBoolConfigVariable(true, &EnableConfigProcessedMetric, false, "Diagnostics.enableConfigProcessedMetric")
	config.RegisterBoolConfigVariable(true, &EnableGatewayMetric, false, "Diagnostics.enableGatewayMetric")
	config.RegisterBoolConfigVariable(true, &EnableRouterMetric, false, "Diagnostics.enableRouterMetric")
	config.RegisterBoolConfigVariable(true, &EnableBatchRouterMetric, false, "Diagnostics.enableBatchRouterMetric")
	config.RegisterBoolConfigVariable(true, &EnableDestinationFailuresMetric, false, "Diagnostics.enableDestinationFailuresMetric")
	Diagnostics = newDiagnostics()
}

// newDiagnostics return new instace of diagnostics
func newDiagnostics() *diagnostics {
	instanceId := config.GetString("INSTANCE_ID", "1")

	client := analytics.New(writekey, endpoint)
	return &diagnostics{
		InstanceId: instanceId,
		Client:     client,
		StartTime:  time.Now(),
		UniqueId:   misc.GetMD5Hash(misc.GetMacAddress()),
	}
}

func (d *diagnostics) Track(event string, properties map[string]interface{}) {
	if EnableDiagnostics {
		properties[StartTime] = d.StartTime
		properties[InstanceId] = d.InstanceId

		_ = d.Client.Enqueue(
			analytics.Track{
				Event:       event,
				Properties:  properties,
				AnonymousId: d.UniqueId,
				UserId:      d.UserId,
			},
		)
	}
}

func (*diagnostics) DisableMetrics(enableMetrics bool) {
	if !enableMetrics {
		EnableServerStartedMetric = false
		EnableConfigProcessedMetric = false
		EnableGatewayMetric = false
		EnableRouterMetric = false
		EnableBatchRouterMetric = false
		EnableDestinationFailuresMetric = false
	}
}

func (d *diagnostics) Identify(properties map[string]interface{}) {
	if EnableDiagnostics {
		// add in traits
		if val, ok := properties[ConfigIdentify]; ok {
			d.UserId = val.(string)
		}
		_ = d.Client.Enqueue(
			analytics.Identify{
				AnonymousId: d.UniqueId,
				UserId:      d.UserId,
			},
		)
	}
}

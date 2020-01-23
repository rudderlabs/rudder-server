package diagnosis

import (
	"github.com/rudderlabs/analytics-go"
	"github.com/rudderlabs/rudder-server/config"
	uuid "github.com/satori/go.uuid"
	"time"
)

const (
	StartTime         = "diagnosis_start_time"
	ServerStart       = "server_start"
	ConfigProcessed   = "config_processed"
	SourcesCount      = "no_of_sources"
	DesitanationCount = "no_of_destinations"
	ServerStarted     = "server_started"
	ConfigIdentify    = "identify"
)

var (
	enableDiagnosis bool
	rudderEndpoint  string
)

var diagnosis Diagnosis

type Diagnosis struct {
	Client    analytics.Client
	StartTime time.Time
	serverId  string
}

func init() {
	enableDiagnosis = config.GetBool("Diagnosis.enableDiagnosis", true)
	rudderEndpoint = config.GetString("Diagnosis.endpoint", "http://localhost:8080")
	config := analytics.Config{
		Endpoint: rudderEndpoint,
	}
	client, _ := analytics.NewWithConfig("1TnQwbNV2QBdOsVlZIeKsvP2cez", config)
	diagnosis.Client = client
	diagnosis.StartTime = time.Now()
	diagnosis.serverId = uuid.NewV4().String()
}

func Track(event string, properties map[string]interface{}) {
	if enableDiagnosis {
		properties[StartTime] = diagnosis.StartTime
		diagnosis.Client.Enqueue(
			analytics.Track{
				Event:      event,
				Properties: properties,
				UserId:     diagnosis.serverId,
			},
		)
	}
}

func Identify(event string, properties map[string]interface{}) {
	if enableDiagnosis {
		properties[StartTime] = diagnosis.StartTime
		diagnosis.Client.Enqueue(
			analytics.Track{
				Event:      event,
				Properties: properties,
				UserId:     diagnosis.serverId,
			},
		)
	}
}

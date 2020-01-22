package diagnosis

import (
	"github.com/rudderlabs/analytics-go"
	"github.com/rudderlabs/rudder-server/config"
	uuid "github.com/satori/go.uuid"
	"time"
)

type Diagnosis struct {
	Client    analytics.Client
	StartTime time.Time
	serverId  string
}

var (
	enableDiagnosis bool
	rudderEndpoint  string
)

var diagnosis Diagnosis

func init() {
	enableDiagnosis = config.GetBool("Diagnosis.enableDiagnosis", true)
	rudderEndpoint = config.GetString("Diagnosis.endpoint", "123")
	config := analytics.Config{
		Endpoint: rudderEndpoint,
	}
	client, _ := analytics.NewWithConfig("123", config)
	diagnosis.Client = client
	diagnosis.StartTime = time.Now()
	diagnosis.serverId = uuid.NewV4().String()
	track()
}

func track() {
	if enableDiagnosis {
		serverStartProperties := make(map[string]interface{})
		serverStartProperties["server_start"] = diagnosis.StartTime
		diagnosis.Client.Enqueue(
			analytics.Track{
				Event:      "config_processed",
				Properties: serverStartProperties,
				UserId:     diagnosis.serverId,
			},
		)
	}
}

//func TrackBackendConfig(config backendconfig.SourcesT) {
//	if enableDiagnosis {
//		configProperties := make(map[string]interface{})
//		configProperties["no_of_sources"] = len(config.Sources)
//		var noOfDestinations int
//		for _, source := range config.Sources {
//			noOfDestinations = noOfDestinations + len(source.Destinations)
//		}
//		configProperties["no_of_destinations"] = noOfDestinations
//		diagnosis.Client.Enqueue(
//			analytics.Track{
//				Event:      "config_processed",
//				Properties: configProperties,
//				UserId:     diagnosis.serverId,
//			},
//		)
//	}
//
//}

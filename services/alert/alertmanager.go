package alert

import (
	"errors"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

var (
	alertProvider       string
	pagerDutyRoutingKey string
	instanceName        string
	victorOpsRoutingKey string
)

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("alert")
}

func loadConfig() {
	alertProvider = config.GetString("ALERT_PROVIDER", "victorops")
	pagerDutyRoutingKey = config.GetString("PG_ROUTING_KEY", "")
	instanceName = config.GetString("INSTANCE_ID", "")
	victorOpsRoutingKey = config.GetString("VICTOROPS_ROUTING_KEY", "")
}

// AlertManager interface
type AlertManager interface {
	Alert(string)
}

// New returns FileManager backed by configured privider
func New() (AlertManager, error) {
	switch alertProvider {
	case "victorops":
		return &VictorOps{
			routingKey:   victorOpsRoutingKey,
			instanceName: instanceName,
		}, nil
	case "pagerduty":
		return &PagerDuty{
			routingKey:   pagerDutyRoutingKey,
			instanceName: instanceName,
		}, nil
	}
	return nil, errors.New("No provider configured for Alert Manager")
}

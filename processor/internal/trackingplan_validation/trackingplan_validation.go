package trackingplan_validation

import (
	"fmt"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

type TPValidator struct {
	config struct {
		destTransformationURL string
	}
	conf *config.Config
	log  logger.Logger
	stat stats.Stats
}

func (t *TPValidator) SendRequest(data interface{}) (interface{}, error) {
	fmt.Println("Sending request to Service A")
	// Add service-specific logic
	return "Response from Service A", nil
}

func NewTPValidator(conf *config.Config, log logger.Logger, stat stats.Stats) *TPValidator {
	handle := &TPValidator{}
	handle.conf = conf
	handle.log = log
	handle.stat = stat
	handle.config.destTransformationURL = handle.conf.GetString("Warehouse.destTransformationURL", "http://localhost:9090")
	return handle
}

func (t *TPValidator) trackingPlanValidationURL() string {
	return t.config.destTransformationURL + "/v0/validate"
}

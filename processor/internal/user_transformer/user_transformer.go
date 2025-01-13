package user_transformer

import (
	"fmt"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

type UserTransformer struct {
	config struct {
		userTransformationURL string
	}
	conf *config.Config
	log  logger.Logger
	stat stats.Stats
}

func (u *UserTransformer) SendRequest(data interface{}) (interface{}, error) {
	fmt.Println("Sending request to Service A")
	// Add service-specific logic
	return "Response from Service A", nil
}

func NewUserTransformer(conf *config.Config, log logger.Logger, stat stats.Stats) *UserTransformer {
	handle := &UserTransformer{}
	handle.conf = conf
	handle.log = log
	handle.stat = stat
	handle.config.userTransformationURL = handle.conf.GetString("Warehouse.userTransformationURL", "http://localhost:9090")
	return handle
}

func (u *UserTransformer) userTransformURL() string {
	return u.config.userTransformationURL + "/customTransform"
}

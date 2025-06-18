package lambda

import (
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

func NewProducer(destination *backendconfig.DestinationT, o common.Opts) (common.Producer, error) {
	return common.NewSwitchingProducer("LAMBDA", pkgLogger, destination, o, NewProducerV1, NewProducerV2)
}

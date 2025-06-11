package lambda

import (
	"encoding/json"
	"errors"
	"io"

	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

type LambdaManager interface {
	io.Closer
	Produce(jsonData json.RawMessage, _ interface{}) (int, string, string)
}

func NewProducer(destination *backendconfig.DestinationT, o common.Opts) (LambdaManager, error) {
	v2Enabled := config.GetReloadableBoolVar(false, "Router.LAMBDA.useAWSV2", "Router.useAWSV2")
	producerV1, err := NewProducerV1(destination, o)
	if err != nil {
		return nil, err
	}
	producerV2, err := NewProducerV2(destination, o)
	if err != nil {
		if v2Enabled.Load() {
			return nil, err
		} else {
			pkgLogger.Error("Error creating producer v2", err)
		}
	}
	return &SwitchingLambdaManager{isV2Enabled: v2Enabled, producerV1: producerV1, producerV2: producerV2}, nil
}

type SwitchingLambdaManager struct {
	isV2Enabled config.ValueLoader[bool]
	producerV1  *LambdaProducerV1
	producerV2  *LambdaProducerV2
}

func (s *SwitchingLambdaManager) Produce(jsonData json.RawMessage, val interface{}) (int, string, string) {
	if s.isV2Enabled.Load() && s.producerV2 != nil {
		return s.producerV2.Produce(jsonData, val)
	}
	return s.producerV1.Produce(jsonData, val)
}

func (s *SwitchingLambdaManager) Close() error {
	var closeErrors []error
	if s.producerV2 != nil {
		closeErrors = append(closeErrors, s.producerV2.Close())
	}
	closeErrors = append(closeErrors, s.producerV1.Close())
	return errors.Join(closeErrors...)
}

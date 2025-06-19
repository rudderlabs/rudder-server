package common

import (
	"encoding/json"
	"errors"
	"io"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

type Producer interface {
	io.Closer
	Produce(jsonData json.RawMessage, _ interface{}) (int, string, string)
}

func NewSwitchingProducer(name string, log logger.Logger, destination *backendconfig.DestinationT, o Opts, v1, v2 func(*backendconfig.DestinationT, Opts) (Producer, error)) (Producer, error) {
	v2Enabled := config.GetReloadableBoolVar(false, "Router."+name+".useAWSV2", "Router.useAWSV2")
	producerV1, err := v1(destination, o)
	if err != nil {
		return nil, err
	}
	producerV2, err := v2(destination, o)
	if err != nil {
		if v2Enabled.Load() {
			return nil, err
		} else {
			log.Error("Error creating producer v2", err)
		}
	}
	return &SwitchingProducer{isV2Enabled: v2Enabled, producerV1: producerV1, producerV2: producerV2}, nil
}

type SwitchingProducer struct {
	isV2Enabled config.ValueLoader[bool]
	producerV1  Producer
	producerV2  Producer
}

func (s *SwitchingProducer) Produce(jsonData json.RawMessage, val interface{}) (int, string, string) {
	if s.isV2Enabled.Load() && s.producerV2 != nil {
		return s.producerV2.Produce(jsonData, val)
	}
	return s.producerV1.Produce(jsonData, val)
}

func (s *SwitchingProducer) Close() error {
	var closeErrors []error
	if s.producerV2 != nil {
		closeErrors = append(closeErrors, s.producerV2.Close())
	}
	closeErrors = append(closeErrors, s.producerV1.Close())
	return errors.Join(closeErrors...)
}

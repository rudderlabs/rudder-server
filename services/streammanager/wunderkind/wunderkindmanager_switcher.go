package wunderkind

import (
	"encoding/json"
	"errors"
	"io"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

type WunderkindManager interface {
	io.Closer
	Produce(jsonData json.RawMessage, _ interface{}) (int, string, string)
}

func NewProducer(conf *config.Config, log logger.Logger) (WunderkindManager, error) {
	v2Enabled := conf.GetReloadableBoolVar(false, "Router.WUNDERKIND.useAWSV2", "Router.useAWSV2")
	producerV1, err := NewProducerV1(conf, log)
	if err != nil {
		return nil, err
	}
	producerV2, err := NewProducerV2(conf, log)
	if err != nil {
		if v2Enabled.Load() {
			return nil, err
		} else {
			log.Errorn("Error creating producer v2", obskit.Error(err))
		}
	}
	return &SwitchingWunderkindManager{isV2Enabled: v2Enabled, producerV1: producerV1, producerV2: producerV2}, nil
}

type SwitchingWunderkindManager struct {
	isV2Enabled config.ValueLoader[bool]
	producerV1  *ProducerV1
	producerV2  *ProducerV2
}

func (s *SwitchingWunderkindManager) Produce(jsonData json.RawMessage, val interface{}) (int, string, string) {
	if s.isV2Enabled.Load() && s.producerV2 != nil {
		return s.producerV2.Produce(jsonData, val)
	}
	return s.producerV1.Produce(jsonData, val)
}

func (s *SwitchingWunderkindManager) Close() error {
	var closeErrors []error
	if s.producerV2 != nil {
		closeErrors = append(closeErrors, s.producerV2.Close())
	}
	closeErrors = append(closeErrors, s.producerV1.Close())
	return errors.Join(closeErrors...)
}

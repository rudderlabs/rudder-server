package kinesis

import (
	"encoding/json"
	"io"

	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

type KinesisManager interface {
	io.Closer
	Produce(jsonData json.RawMessage, _ interface{}) (int, string, string)
}

func NewProducer(destination *backendconfig.DestinationT, o common.Opts) (KinesisManager, error) {
	v2Enabled := config.GetReloadableBoolVar(false, "Router.KINESIS.v2Enabled", "Router.v2Enabled")
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
	return &SwitchingKinesisManager{isV2Enabled: v2Enabled.Load(), producerV1: producerV1, producerV2: producerV2}, nil
}

type SwitchingKinesisManager struct {
	isV2Enabled bool
	producerV1  *KinesisProducerV1
	producerV2  *KinesisProducerV2
}

func (s *SwitchingKinesisManager) Produce(jsonData json.RawMessage, val interface{}) (int, string, string) {
	if s.isV2Enabled {
		return s.producerV2.Produce(jsonData, val)
	}
	return s.producerV1.Produce(jsonData, val)
}

func (s *SwitchingKinesisManager) Close() error {
	if s.isV2Enabled {
		return s.producerV2.Close()
	}
	return s.producerV1.Close()
}

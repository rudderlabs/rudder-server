package processor

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-schemas/go/stream"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type ReceivedAtProcessor struct {
	BaseProcessor
	OnError func(stream.MessageProperties, error)
}

func (p *ReceivedAtProcessor) Process(payload []byte, properties stream.MessageProperties) ([]byte, error) {
	var err error
	payload, err = fillReceivedAt(payload, properties.ReceivedAt)
	if err != nil {
		p.OnError(properties, fmt.Errorf("filling receivedAt: %w", err))
		return nil, fmt.Errorf("filling receivedAt: %w", err)
	}
	return p.ProcessNext(payload, properties)
}

func fillReceivedAt(event []byte, receivedAt time.Time) ([]byte, error) {
	if !gjson.GetBytes(event, "receivedAt").Exists() {
		return sjson.SetBytes(event, "receivedAt", receivedAt.Format(misc.RFC3339Milli))
	}
	return event, nil
}

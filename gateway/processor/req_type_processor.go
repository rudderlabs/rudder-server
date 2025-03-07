package processor

import (
	"errors"
	"fmt"

	"github.com/rudderlabs/rudder-server/gateway/response"

	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-schemas/go/stream"
)

type ReqTypeProcessor struct {
	BaseProcessor
	OnError func(stream.MessageProperties, error)
}

func (p *ReqTypeProcessor) Process(payload []byte, properties stream.MessageProperties) ([]byte, error) {
	reqType := properties.RequestType
	if reqType != "" {
		switch reqType {
		case "batch", "replay", "retl", "import":
		default:
			var err error
			payload, err = sjson.SetBytes(payload, "type", reqType)
			if err != nil {
				p.OnError(properties, fmt.Errorf("setting req type: %w", err))
				return nil, errors.New(response.NotRudderEvent)
			}
		}
	}
	return p.ProcessNext(payload, properties)
}

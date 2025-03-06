package processor

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
	kituuid "github.com/rudderlabs/rudder-go-kit/uuid"
	"github.com/rudderlabs/rudder-schemas/go/stream"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type RudderIDProcessor struct {
	BaseProcessor
	OnError func(stream.MessageProperties, error)
}

func (p *RudderIDProcessor) Process(payload []byte, properties stream.MessageProperties) ([]byte, error) {
	anonIDFromReq := sanitizeAndTrim(gjson.GetBytes(payload, "anonymousId").String())
	userIDFromReq := sanitizeAndTrim(gjson.GetBytes(payload, "userId").String())
	rudderId, err := getRudderId(userIDFromReq, anonIDFromReq)
	if err != nil {
		p.OnError(properties, fmt.Errorf("getting rudderId: %w", err))
		return nil, errors.New(response.NotRudderEvent)
	}
	payload, err = sjson.SetBytes(payload, "rudderId", rudderId)
	if err != nil {
		p.OnError(properties, fmt.Errorf("setting rudderId: %w", err))
		return nil, errors.New(response.NotRudderEvent)
	}
	return p.ProcessNext(payload, properties)
}

func getRudderId(userIDFromReq, anonIDFromReq string) (uuid.UUID, error) {
	rudderId, err := kituuid.GetMD5UUID(userIDFromReq + ":" + anonIDFromReq)
	if err != nil {
		return rudderId, err
	}
	return rudderId, nil
}

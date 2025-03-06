package processor

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/rudderlabs/rudder-go-kit/sanitize"
	"github.com/rudderlabs/rudder-schemas/go/stream"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type MessageIDProcessor struct {
	BaseProcessor
	OnError func(stream.MessageProperties, error)
}

func (p *MessageIDProcessor) Process(payload []byte, properties stream.MessageProperties) ([]byte, error) {
	messageID, changed := getMessageID(payload)
	if changed {
		var err error
		payload, err = sjson.SetBytes(payload, "messageId", messageID)
		if err != nil {
			p.OnError(properties, fmt.Errorf("setting messageID: %w", err))
			return nil, errors.New(response.NotRudderEvent)
		}
	}
	return p.ProcessNext(payload, properties)
}

// getMessageID returns the messageID from the event payload.
// If the messageID is not present, it generates a new one.
// It also returns a boolean indicating if the messageID was changed.
func getMessageID(event []byte) (string, bool) {
	messageID := gjson.GetBytes(event, "messageId").String()
	sanitizedMessageID := sanitizeAndTrim(messageID)
	if sanitizedMessageID == "" {
		return uuid.New().String(), true
	}
	return sanitizedMessageID, messageID != sanitizedMessageID
}

func sanitizeAndTrim(str string) string {
	return strings.TrimSpace(sanitize.Unicode(str))
}

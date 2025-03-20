package validator

import (
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-schemas/go/stream"
)

type MessageIDValidator struct{}

func (p *MessageIDValidator) ValidatorName() string {
	return "MessageIDValidator"
}

func (p *MessageIDValidator) Validate(payload []byte, properties *stream.MessageProperties) (bool, error) {
	return gjson.GetBytes(payload, "messageId").String() != "", nil
}

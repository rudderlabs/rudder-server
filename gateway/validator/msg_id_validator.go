package validator

import (
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-schemas/go/stream"
)

type messageIDValidator struct{}

func newMessageIDValidator() *messageIDValidator {
	return &messageIDValidator{}
}

func (p *messageIDValidator) ValidatorName() string {
	return "messageID"
}

func (p *messageIDValidator) Validate(payload []byte, properties *stream.MessageProperties) (bool, error) {
	return gjson.GetBytes(payload, "messageId").String() != "", nil
}

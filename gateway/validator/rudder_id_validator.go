package validator

import (
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-schemas/go/stream"
)

type rudderIDValidator struct{}

func newRudderIDValidator() *rudderIDValidator {
	return &rudderIDValidator{}
}

func (p *rudderIDValidator) ValidatorName() string {
	return "RudderID"
}

func (p *rudderIDValidator) Validate(payload []byte, properties *stream.MessageProperties) (bool, error) {
	return gjson.GetBytes(payload, "rudderId").Exists(), nil
}

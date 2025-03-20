package validator

import (
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-schemas/go/stream"
)

type RudderIDValidator struct{}

func (p *RudderIDValidator) ValidatorName() string {
	return "RudderIDValidator"
}

func (p *RudderIDValidator) Validate(payload []byte, properties *stream.MessageProperties) (bool, error) {
	return gjson.GetBytes(payload, "rudderId").Exists(), nil
}

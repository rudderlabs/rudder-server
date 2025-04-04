package validator

import (
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-schemas/go/stream"
)

type requestIPValidator struct{}

func newRequestIPValidator() *requestIPValidator {
	return &requestIPValidator{}
}

func (e *requestIPValidator) ValidatorName() string {
	return "RequestIP"
}

func (p *requestIPValidator) Validate(payload []byte, properties *stream.MessageProperties) (bool, error) {
	return gjson.GetBytes(payload, "request_ip").Exists(), nil
}

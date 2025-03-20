package validator

import (
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-schemas/go/stream"
)

type RequestIPValidator struct{}

func (e *RequestIPValidator) ValidatorName() string {
	return "RequestIPValidator"
}

func (p *RequestIPValidator) Validate(payload []byte, properties *stream.MessageProperties) (bool, error) {
	return gjson.GetBytes(payload, "request_ip").Exists(), nil
}

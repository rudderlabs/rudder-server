package validator

import (
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-schemas/go/stream"
)

type receivedAtValidator struct{}

func newReceivedAtValidator() *receivedAtValidator {
	return &receivedAtValidator{}
}

func (e *receivedAtValidator) ValidatorName() string {
	return "ReceivedAt"
}

func (e *receivedAtValidator) Validate(payload []byte, properties *stream.MessageProperties) (bool, error) {
	return gjson.GetBytes(payload, "receivedAt").Exists(), nil
}

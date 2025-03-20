package validator

import (
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-schemas/go/stream"
)

type ReceivedAtValidator struct{}

func (e *ReceivedAtValidator) ValidatorName() string {
	return "ReceivedAtValidator"
}

func (e *ReceivedAtValidator) Validate(payload []byte, properties *stream.MessageProperties) (bool, error) {
	return gjson.GetBytes(payload, "receivedAt").Exists(), nil
}

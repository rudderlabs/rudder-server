package validator

import (
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-schemas/go/stream"
)

type ReqTypeValidator struct{}

func (p *ReqTypeValidator) ValidatorName() string {
	return "ReqTypeValidator"
}

func (p *ReqTypeValidator) Validate(payload []byte, properties *stream.MessageProperties) (bool, error) {
	reqType := properties.RequestType
	if reqType != "" {
		switch reqType {
		case "batch", "replay", "retl", "import":
		default:
			return gjson.GetBytes(payload, "type").Exists(), nil
		}
	}
	return true, nil
}

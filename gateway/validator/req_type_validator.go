package validator

import (
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-schemas/go/stream"
)

type reqTypeValidator struct{}

func newReqTypeValidator() *reqTypeValidator {
	return &reqTypeValidator{}
}

func (p *reqTypeValidator) ValidatorName() string {
	return "ReqType"
}

func (p *reqTypeValidator) Validate(payload []byte, properties *stream.MessageProperties) (bool, error) {
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

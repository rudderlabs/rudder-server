package validator

import (
	"github.com/rudderlabs/rudder-schemas/go/stream"
)

type msgPropertiesValidator struct {
	validateFn func(*stream.MessageProperties) error
}

func newMsgPropertiesValidator(validateFn func(*stream.MessageProperties) error) *msgPropertiesValidator {
	return &msgPropertiesValidator{
		validateFn: validateFn,
	}
}

func (p *msgPropertiesValidator) ValidatorName() string {
	return "msgProperties"
}

func (p *msgPropertiesValidator) Validate(payload []byte, properties *stream.MessageProperties) (bool, error) {
	if err := p.validateFn(properties); err != nil {
		return false, err
	}
	return true, nil
}

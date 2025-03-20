package validator

import (
	"github.com/rudderlabs/rudder-schemas/go/stream"
)

type MsgPropertiesValidator struct {
	validateFn func(*stream.MessageProperties) error
}

func (p *MsgPropertiesValidator) ValidatorName() string {
	return "MsgPropertiesValidator"
}

func (p *MsgPropertiesValidator) Validate(payload []byte, properties *stream.MessageProperties) (bool, error) {
	if err := p.validateFn(properties); err != nil {
		return false, err
	}
	return true, nil
}

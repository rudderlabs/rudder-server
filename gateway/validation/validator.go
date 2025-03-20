package validator

import (
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-schemas/go/stream"
)

// PayloadValidator defines an interface for validating payloads and retrieving the validator's name.
type PayloadValidator interface {
	Validate(payload []byte, properties *stream.MessageProperties) (bool, error)
	ValidatorName() string
}

// ValidatorMediator centralizes the orchestration of multiple payload validation processes.
type ValidatorMediator struct {
	log        logger.Logger
	validators []PayloadValidator
}

// NewValidateMediator creates a new ValidatorMediator with default validators.
func NewValidateMediator(log logger.Logger, validatorFn func(properties *stream.MessageProperties) error) *ValidatorMediator {
	return &ValidatorMediator{
		log: log,
		validators: []PayloadValidator{
			&MsgPropertiesValidator{
				validateFn: validatorFn,
			},
			&MessageIDValidator{},
			&ReqTypeValidator{},
			&ReceivedAtValidator{},
			&RequestIPValidator{},
			&RudderIDValidator{},
		},
	}
}

// Validate runs the payload through all registered validators.
func (m *ValidatorMediator) Validate(payload []byte, properties *stream.MessageProperties) (bool, error) {
	for _, validator := range m.validators {
		if ok, err := validator.Validate(payload, properties); err != nil || !ok {
			loggerFields := properties.LoggerFields()
			loggerFields = append(loggerFields,
				logger.NewStringField("validator", validator.ValidatorName()),
				obskit.Error(err))
			m.log.Errorn("failed to validate", loggerFields...)
			return false, err
		}
	}
	return true, nil
}

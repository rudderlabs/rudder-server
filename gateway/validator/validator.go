package validator

import (
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-schemas/go/stream"
)

// payloadValidator defines an interface for validating payloads and retrieving the validator's name.
type payloadValidator interface {
	Validate(payload []byte, properties *stream.MessageProperties) (bool, error)
	ValidatorName() string
}

// Mediator centralizes the orchestration of multiple payload validator processes.
type Mediator struct {
	log        logger.Logger
	validators []payloadValidator
}

// NewValidateMediator creates a new ValidatorMediator with default validators.
func NewValidateMediator(log logger.Logger, validatorFn func(properties *stream.MessageProperties) error) *Mediator {
	return &Mediator{
		log: log.Withn(logger.NewStringField("component", "validator")),
		validators: []payloadValidator{
			newMsgPropertiesValidator(validatorFn),
			newMessageIDValidator(),
			newReqTypeValidator(),
			newReceivedAtValidator(),
			newRequestIPValidator(),
			newRudderIDValidator(),
		},
	}
}

// Validate runs the payload through all registered validators.
func (m *Mediator) Validate(payload []byte, properties *stream.MessageProperties) (bool, error) {
	for _, validator := range m.validators {
		if ok, err := validator.Validate(payload, properties); err != nil || !ok {
			loggerFields := properties.LoggerFields()
			loggerFields = append(loggerFields,
				logger.NewStringField("validator", validator.ValidatorName()))
			if err != nil {
				loggerFields = append(loggerFields, obskit.Error(err))
			}
			m.log.Errorn("failed to validate", loggerFields...)
			return false, err
		}
	}
	return true, nil
}

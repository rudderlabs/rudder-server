package v2

import (
	"errors"
	"fmt"

	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
)

// TypeMessageError is an error that has both a type and a message.
type TypeMessageError struct {
	Type    string
	Message string
}

func (e *TypeMessageError) Error() string {
	return fmt.Sprintf("type: %s, message: %s", e.Type, e.Message)
}

func (e *TypeMessageError) Unwrap() error {
	switch e.Type {
	case common.RefTokenInvalidGrant:
		return common.ErrInvalidGrant
	default:
		return errors.New(e.Type)
	}
}

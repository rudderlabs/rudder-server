package warehouse

import (
	"errors"
	"fmt"
)

var (
	ErrIncompatibleSchemaConv = errors.New("incompatible schema conversion")
	ErrSchemaConvNotSupported = errors.New("schema conversion not supported")
)

type InvalidDestinationCredErr struct {
	Base      error
	Operation string
}

func (err InvalidDestinationCredErr) Error() string {
	return fmt.Sprintf("Invalid destination creds, failed for operation: %s with err: \n%s", err.Operation, err.Base.Error())
}

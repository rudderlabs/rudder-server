package warehouse

import (
	"errors"
	"fmt"
)

var ErrSchemaChange = errors.New("incompatible conversion of data")

type InvalidDestinationCredErr struct {
	Base      error
	Operation string
}

func (err InvalidDestinationCredErr) Error() string {
	return fmt.Sprintf("Invalid destination creds, failed for operation: %s with err: \n%s", err.Operation, err.Base.Error())
}

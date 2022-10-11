package warehouse

import "fmt"

type InvalidDestinationCredErr struct {
	Base      error
	Operation string
}

func (err InvalidDestinationCredErr) Error() string {
	return fmt.Sprintf("Invalid destination creds, failed with err: %s for operation: %s", err.Base.Error(), err.Operation)
}

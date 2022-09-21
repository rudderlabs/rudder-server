package warehouse

import (
	"errors"
	"fmt"

	"github.com/rudderlabs/rudder-server/warehouse/utils"
)

type ErrorCode int

type DestinationType string

type SyncsErr struct {
	Base            error
	Code            int
	Operation       string
	DestinationType string
}

var skipErrorCodesMap = map[DestinationType][]ErrorCode{
	warehouseutils.SNOWFLAKE: {
		42501,
	},
}

func (err *SyncsErr) Error() string {
	if err.Operation != "" {
		return fmt.Sprintf("failed with err: %s for operation: %s", err.Base.Error(), err.Operation)
	}
	return fmt.Sprintf("failed with err: %s", err.Base.Error())
}

func (err *SyncsErr) canSkipError() bool {
	if err.DestinationType == "" {
		return false
	}
	for _, errorCode := range skipErrorCodesMap[DestinationType(err.DestinationType)] {
		if ErrorCode(err.Code) == errorCode {
			return true
		}
	}
	return false
}

func extractSyncError(err error) error {
	var syncsErr *SyncsErr
	if errors.As(err, &syncsErr) {
		return syncsErr
	}

	return &SyncsErr{
		Base: err,
	}
}

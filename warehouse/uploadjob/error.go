package uploadjob

import (
	"strings"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
)

type ErrorHandler struct {
	Mapper manager.Manager
}

func (h ErrorHandler) MatchUploadJobErrorType(err error) string {
	if err == nil {
		return ""
	}

	errStr := err.Error()
	switch {
	case strings.Contains(errStr, "account-locked"):
		return "account-locked"
	case strings.Contains(errStr, "invalid-credentials"):
		return "invalid-credentials"
	case strings.Contains(errStr, "permission-denied"):
		return "permission-denied"
	case strings.Contains(errStr, "quota-exceeded"):
		return "quota-exceeded"
	case strings.Contains(errStr, "rate-limit-exceeded"):
		return "rate-limit-exceeded"
	case strings.Contains(errStr, "resource-not-found"):
		return "resource-not-found"
	case strings.Contains(errStr, "service-unavailable"):
		return "service-unavailable"
	case strings.Contains(errStr, "timeout"):
		return "timeout"
	case strings.Contains(errStr, "too-many-requests"):
		return "too-many-requests"
	case strings.Contains(errStr, "unauthorized"):
		return "unauthorized"
	case strings.Contains(errStr, "validation-error"):
		return "validation-error"
	default:
		return "unknown"
	}
}

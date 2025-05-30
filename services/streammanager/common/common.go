//go:generate mockgen --build_flags=--mod=mod -destination=../../../mocks/services/streammanager/common/mock_streammanager.go -package mock_streammanager github.com/rudderlabs/rudder-server/services/streammanager/common StreamProducer

package common

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/aws/smithy-go"
)

type StreamProducer interface {
	io.Closer
	Produce(jsonData json.RawMessage, destConfig interface{}) (int, string, string)
}

type Opts struct {
	Timeout time.Duration
}

func mapErrorMessageToStatusCode(errorMessage string, defaultStatusCode int) int {
	if strings.Contains(errorMessage, "Throttling") {
		return 429
	}
	if strings.Contains(errorMessage, "RequestExpired") {
		return 500
	}
	return defaultStatusCode
}

func ParseAWSError(err error) (statusCode int, respStatus, responseMessage string) {
	statusCode = 500
	respStatus = "Failure"
	responseMessage = err.Error()

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		responseMessage = apiErr.ErrorMessage()
		respStatus = apiErr.ErrorCode()
		if httpErr, ok := err.(interface{ HTTPStatusCode() int }); ok {
			statusCode = httpErr.HTTPStatusCode()
		}
	} else {
		var opErr *smithy.OperationError
		if errors.As(err, &opErr) {
			responseMessage = opErr.Error()
			if apiErr := extractAPIErrorCode(opErr.Err); apiErr != "" {
				respStatus = apiErr
			}
			if httpErr, ok := opErr.Err.(interface{ HTTPStatusCode() int }); ok {
				statusCode = httpErr.HTTPStatusCode()
			}
		}
	}

	statusCode = mapErrorMessageToStatusCode(responseMessage, statusCode)
	return statusCode, respStatus, responseMessage
}

// extractAPIErrorCode attempts to extract the AWS API error code from various error types
func extractAPIErrorCode(err error) string {
	type awsErrorCode interface {
		ErrorCode() string
	}

	if coder, ok := err.(awsErrorCode); ok {
		return coder.ErrorCode()
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode()
	}

	return ""
}

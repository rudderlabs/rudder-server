//go:generate mockgen --build_flags=--mod=mod -destination=../../../mocks/services/streammanager/common/mock_streammanager.go -package mock_streammanager github.com/rudderlabs/rudder-server/services/streammanager/common StreamProducer

package common

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
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
		// aws returns  "ThrottlingException"
		// for throttling requests server will retry
		return 429
	}
	if strings.Contains(errorMessage, "RequestExpired") {
		// Retryable
		return 500
	}
	return defaultStatusCode
}

func ParseAWSError(err error) (statusCode int, respStatus, responseMessage string) {
	statusCode = 500
	respStatus = "Failure"
	responseMessage = err.Error()
	if reqErr, ok := err.(awserr.RequestFailure); ok {
		responseMessage = reqErr.Error()
		respStatus = reqErr.Code()
		statusCode = reqErr.StatusCode()
	}

	statusCode = mapErrorMessageToStatusCode(responseMessage, statusCode)
	return statusCode, respStatus, responseMessage
}

func getStatusCodeFromFault(fault smithy.ErrorFault) int {
	switch fault {
	case smithy.FaultClient:
		return 400
	case smithy.FaultServer:
		return 500
	}
	return 500
}

func ParseAWSErrorV2(err error) (statusCode int, respStatus, responseMessage string) {
	statusCode = 500
	respStatus = "Failure"
	responseMessage = err.Error()

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		responseMessage = apiErr.ErrorMessage()
		respStatus = apiErr.ErrorCode()
		fault := apiErr.ErrorFault()
		statusCode = getStatusCodeFromFault(fault)
	} else {
		var opErr *smithy.OperationError
		if errors.As(err, &opErr) {
			responseMessage = opErr.Unwrap().Error()
			statusCode = mapErrorMessageToStatusCode(responseMessage, 400)
			respStatus = "Failure"
		}
	}

	return statusCode, respStatus, responseMessage
}

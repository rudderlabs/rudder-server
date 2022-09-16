//go:generate mockgen --build_flags=--mod=mod -destination=../../../mocks/services/streammanager/common/mock_streammanager.go -package mock_streammanager github.com/rudderlabs/rudder-server/services/streammanager/common StreamProducer

package common

import (
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
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

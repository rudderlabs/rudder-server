package errors

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
)

// Error messages
const (
	// Driver errors
	ErrNotImplemented           = "not implemented"
	ErrTransactionsNotSupported = "transactions are not supported"
	ErrParametersNotSupported   = "query parameters are not supported"
	ErrReadQueryStatus          = "could not read query status"
	ErrSentinelTimeout          = "sentinel timed out waiting for operation to complete"

	// Request error messages (connection, authentication, network error)
	ErrCloseConnection = "failed to close connection"
	ErrThriftClient    = "error initializing thrift client"
	ErrInvalidURL      = "invalid URL"

	ErrNoAuthenticationMethod = "no authentication method set"
	ErrInvalidDSNFormat       = "invalid DSN: invalid format"
	ErrInvalidDSNPort         = "invalid DSN: invalid DSN port"
	ErrInvalidDSNPATIsEmpty   = "invalid DSN: empty token"
	ErrBasicAuthNotSupported  = "invalid DSN: basic auth not enabled"
	ErrInvalidDSNMaxRows      = "invalid DSN: maxRows param is not an integer"
	ErrInvalidDSNTimeout      = "invalid DSN: timeout param is not an integer"

	// Execution error messages (query failure)
	ErrQueryExecution = "failed to execute query"
	ErrLinkExpired    = "link expired"
)

func InvalidDSNFormat(param string, value string, expected string) string {
	return fmt.Sprintf("invalid DSN: param %s with value %s is not of type %s", param, value, expected)
}

func ErrInvalidOperationState(state string) string {
	return fmt.Sprintf("invalid operation state %s. This should not have happened", state)
}

func ErrUnexpectedOperationState(state string) string {
	return fmt.Sprintf("unexpected operation state %s", state)
}

// value to be used with errors.Is() to determine if an error chain contains a request error
var RequestError error = errors.New("Request Error")

// value to be used with errors.Is() to determine if an error chain contains a driver error
var DriverError error = errors.New("Driver Error")

// value to be used with errors.Is() to determine if an error chain contains an execution error
var ExecutionError error = errors.New("Execution Error")

// value to be used with errors.Is() to determine if an error chain contains any databricks error
var DatabricksError error = errors.New("Databricks Error")

// Base interface for driver errors
type DBError interface {
	// Descriptive message describing the error
	Error() string

	// User specified id to track what happens under a request. Useful to track multiple connections in the same request.
	// Appears in log messages as field corrId.  See driverctx.NewContextWithCorrelationId()
	CorrelationId() string

	// Internal id to track what happens under a connection. Connections can be reused so this would track across queries.
	// Appears in log messages as field connId.
	ConnectionId() string

	// Stack trace associated with the error.  May be nil.
	StackTrace() errors.StackTrace

	// Underlying causative error. May be nil.
	Cause() error

	IsRetryable() bool

	RetryAfter() time.Duration
}

// An error that is caused by an invalid request.
// Example: permission denied, or the user tries to access a warehouse that doesn’t exist
type DBRequestError interface {
	DBError
}

// A fault that is caused by Databricks services
type DBDriverError interface {
	DBError
}

// Any error that occurs after the SQL statement has been accepted (e.g. SQL syntax error).
type DBExecutionError interface {
	DBError

	// Internal id to track what happens under a query.
	// Appears in log messages as field queryId.
	QueryId() string

	// Optional portable error identifier across SQL engines.
	// See https://github.com/apache/spark/tree/master/core/src/main/resources/error#ansiiso-standard
	SqlState() string
}

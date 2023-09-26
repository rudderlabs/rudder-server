package errors

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/databricks/databricks-sql-go/driverctx"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/pkg/errors"
)

// value to use with errors.Is() to determine if an error
// chain contains a retryable error
var RetryableError error = errors.New("Retryable Error")

// base databricks error
type databricksError struct {
	err           error
	correlationId string
	connectionId  string
	errType       string
	isRetryable   bool
	retryAfter    time.Duration
}

var _ error = (*databricksError)(nil)

type stackTracer interface {
	StackTrace() errors.StackTrace
}

func newDatabricksError(ctx context.Context, msg string, err error) databricksError {
	// create an error with the new message
	if err == nil {
		err = errors.New(msg)
	} else {
		err = errors.WithMessage(err, msg)
	}

	// if the source error does not have a stack trace in its
	// error chain add a stack trace
	var st stackTracer
	if ok := errors.As(err, &st); !ok {
		err = errors.WithStack(err)
	}

	// If the error chain contains an instance of retryableError
	// set the flag and retryAfter value.
	var retryable bool = false
	var retryAfter time.Duration
	if errors.Is(err, RetryableError) {
		retryable = true
		var re retryableError
		if ok := errors.As(err, &re); ok {
			retryAfter = re.RetryAfter()
		}
	}

	return databricksError{
		err:           err,
		correlationId: driverctx.CorrelationIdFromContext(ctx),
		connectionId:  driverctx.ConnIdFromContext(ctx),
		errType:       "unknown",
		isRetryable:   retryable,
		retryAfter:    retryAfter,
	}
}

func (e databricksError) Error() string {
	return fmt.Sprintf("databricks: %s: %s", e.errType, e.err.Error())
}

func (e databricksError) Cause() error {
	return e.err
}

func (e databricksError) StackTrace() errors.StackTrace {
	var st stackTracer
	if ok := errors.As(e.err, &st); ok {
		return st.StackTrace()
	}

	return nil
}

func (e databricksError) CorrelationId() string {
	return e.correlationId
}

func (e databricksError) ConnectionId() string {
	return e.connectionId
}

func (e databricksError) Is(err error) bool {
	return err == dbsqlerr.DatabricksError
}

func (e databricksError) IsRetryable() bool {
	return e.isRetryable
}

func (e databricksError) RetryAfter() time.Duration {
	return e.retryAfter
}

// driverError are issues with the driver or server, e.g. not supported operations, driver specific non-recoverable failures
type driverError struct {
	databricksError
}

var _ dbsqlerr.DBDriverError = (*driverError)(nil)

func (e driverError) Is(err error) bool {
	return err == dbsqlerr.DriverError || e.databricksError.Is(err)
}

func (e driverError) Unwrap() error {
	return e.err
}

func NewDriverError(ctx context.Context, msg string, err error) *driverError {
	dbErr := newDatabricksError(ctx, msg, err)
	dbErr.errType = "driver error"
	return &driverError{databricksError: dbErr}
}

// requestError are errors caused by invalid requests, e.g. permission denied, warehouse not found
type requestError struct {
	databricksError
}

var _ dbsqlerr.DBRequestError = (*requestError)(nil)

func (e requestError) Is(err error) bool {
	return err == dbsqlerr.RequestError || e.databricksError.Is(err)
}

func (e requestError) Unwrap() error {
	return e.err
}

func NewRequestError(ctx context.Context, msg string, err error) *requestError {
	dbErr := newDatabricksError(ctx, msg, err)
	dbErr.errType = "request error"
	return &requestError{databricksError: dbErr}
}

// executionError are errors occurring after the query has been submitted, e.g. invalid syntax, missing table, etc.
type executionError struct {
	databricksError
	queryId  string
	sqlState string
}

var _ dbsqlerr.DBExecutionError = (*executionError)(nil)

func (e executionError) Is(err error) bool {
	return err == dbsqlerr.ExecutionError || e.databricksError.Is(err)
}

func (e executionError) Unwrap() error {
	return e.err
}

func (e executionError) QueryId() string {
	return e.queryId
}

func (e executionError) SqlState() string {
	return e.sqlState
}

func NewExecutionError(ctx context.Context, msg string, err error, opStatusResp *cli_service.TGetOperationStatusResp) *executionError {
	dbErr := newDatabricksError(ctx, msg, err)
	dbErr.errType = "execution error"
	var sqlState string
	if opStatusResp != nil {
		sqlState = opStatusResp.GetSqlState()
	}

	return &executionError{databricksError: dbErr, queryId: driverctx.QueryIdFromContext(ctx), sqlState: sqlState}
}

// wraps an error and adds trace if not already present
func WrapErr(err error, msg string) error {
	var st stackTracer
	if ok := errors.As(err, &st); ok {
		// wrap passed in error in a new error with the message
		return errors.WithMessage(err, msg)
	}

	// wrap passed in error in errors with the message and a stack trace
	return errors.Wrap(err, msg)
}

// adds a stack trace if not already present
func WrapErrf(err error, format string, args ...interface{}) error {
	var st stackTracer
	if ok := errors.As(err, &st); ok {
		// wrap passed in error in a new error with the formatted message
		return errors.WithMessagef(err, format, args...)
	}

	// wrap passed in error in errors with the formatted message and a stack trace
	return errors.Wrapf(err, format, args...)
}

type retryableError struct {
	err        error
	retryAfter time.Duration
}

func (e retryableError) Is(err error) bool {
	return err == RetryableError
}

func (e retryableError) Unwrap() error {
	return e.err
}

func (e retryableError) Error() string {
	return fmt.Sprintf("databricks: retryableError: %s", e.err.Error())
}

func (e retryableError) RetryAfter() time.Duration {
	return e.retryAfter
}

func NewRetryableError(err error, retryAfterHdr string) error {
	if err == nil {
		err = errors.New("")
	}

	var st stackTracer
	if ok := errors.As(err, &st); !ok {
		err = errors.WithStack(err)
	}

	var retryAfter time.Duration
	if nSeconds, err := strconv.ParseInt(retryAfterHdr, 10, 64); err == nil {
		retryAfter = time.Second * time.Duration(nSeconds)
	}

	return retryableError{err: err, retryAfter: retryAfter}
}

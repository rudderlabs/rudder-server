package restapi

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
)

// Error codes returned by the Snowpipe Streaming HP REST API that callers act on.
const (
	ErrCodeTableDoesNotExist      = "ERR_TABLE_DOES_NOT_EXIST_NOT_AUTHORIZED"
	ErrCodeSchemaDoesNotExist     = "ERR_SCHEMA_DOES_NOT_EXIST_OR_NOT_AUTHORIZED"
	ErrCodePipeDoesNotExist       = "ERR_PIPE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED" // the table was dropped or replaced
	ErrCodeStaleContinuationToken = "STALE_CONTINUATION_TOKEN_SEQUENCER"
	ErrCodeRequestBodyTooLarge    = "REQUEST_BODY_TOO_LARGE"
)

// ChannelStatusSuccess is the status code of a healthy channel.
const ChannelStatusSuccess = "SUCCESS"

const (
	defaultPipeSuffix                 = "-STREAMING"
	maxRequestBodyBytes               = 4 * 1024 * 1024 // 4 MiB, compressed: larger bodies are rejected with 413
	contentTypeNDJSON                 = "application/x-ndjson"
	contentTypeJSON                   = "application/json"
	headerSnowflakeAuthorizationToken = "X-Snowflake-Authorization-Token-Type"
)

type (
	// PipeRef identifies a pipe. Every table has a default pipe named <TABLE>-STREAMING, which Snowflake creates on first use.
	PipeRef struct {
		Database string
		Schema   string
		Pipe     string
	}

	// ChannelRef identifies a channel on a pipe. Channel names are case-sensitive.
	ChannelRef struct {
		PipeRef
		Channel string
	}

	// ChannelStatus is the status of a channel, as returned by open channel and bulk channel status.
	ChannelStatus struct {
		ChannelName                string  `json:"channel_name"`
		ChannelStatusCode          string  `json:"channel_status_code"`
		DatabaseName               string  `json:"database_name"`
		SchemaName                 string  `json:"schema_name"`
		PipeName                   string  `json:"pipe_name"`
		LastCommittedOffsetToken   *string `json:"last_committed_offset_token"`
		RowsInserted               int64   `json:"rows_inserted"`
		RowsParsed                 int64   `json:"rows_parsed"`
		RowsErrorCount             int64   `json:"rows_error_count"`
		LastErrorOffsetUpperBound  *string `json:"last_error_offset_upper_bound"`
		LastErrorMessage           *string `json:"last_error_message"`
		LastErrorTimestampMs       *int64  `json:"last_error_timestamp_ms"`
		CreatedOnMs                int64   `json:"created_on_ms"`
		SnowflakeAvgProcessingTime *int64  `json:"snowflake_avg_processing_latency_ms"`
	}

	// OpenChannelResponse is the response of opening (or reopening) a channel.
	OpenChannelResponse struct {
		NextContinuationToken string        `json:"next_continuation_token"`
		ChannelStatus         ChannelStatus `json:"channel_status"`
	}

	// AppendRowsRequest is one append to a channel.
	AppendRowsRequest struct {
		// ContinuationToken must be the latest token returned for the channel: an older token makes Snowflake
		// silently drop the rows while still answering 200.
		ContinuationToken string
		StartOffsetToken  string
		EndOffsetToken    string
		// Body is zstd-compressed NDJSON, at most 4 MiB. See [Chunker].
		Body []byte
	}

	// AppendRowsResponse is the response of appending rows to a channel.
	AppendRowsResponse struct {
		NextContinuationToken string `json:"next_continuation_token"`
	}

	bulkChannelStatusRequest struct {
		ChannelNames []string `json:"channel_names"`
	}

	bulkChannelStatusResponse struct {
		ChannelStatuses map[string]ChannelStatus `json:"channel_statuses"`
	}
)

// DefaultPipe returns the reference to the default pipe of a table.
func DefaultPipe(database, schema, table string) PipeRef {
	return PipeRef{Database: database, Schema: schema, Pipe: table + defaultPipeSuffix}
}

// CommittedOffset returns the last committed offset token, or an empty string if nothing has been committed yet.
func (s *ChannelStatus) CommittedOffset() string {
	if s.LastCommittedOffsetToken == nil {
		return ""
	}
	return *s.LastCommittedOffsetToken
}

// Healthy reports whether the channel can accept appends.
func (s *ChannelStatus) Healthy() bool {
	return s.ChannelStatusCode == ChannelStatusSuccess
}

// APIError is an error response from Snowflake.
type APIError struct {
	StatusCode int
	Code       string
	Message    string
}

func (e *APIError) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("snowflake responded with status %d: %s", e.StatusCode, e.Message)
	}
	return fmt.Sprintf("snowflake responded with status %d, code %s: %s", e.StatusCode, e.Code, e.Message)
}

// retryable reports whether the same request may succeed if sent again.
// An unstructured 404 (no error code) comes from Snowflake's edge rather than the ingest service, and is transient.
func (e *APIError) retryable() bool {
	switch {
	case e.StatusCode == http.StatusRequestTimeout,
		e.StatusCode == http.StatusTooManyRequests,
		e.StatusCode == 499, // client closed request, as reported by Snowflake's proxy
		e.StatusCode >= http.StatusInternalServerError:
		return true
	case e.StatusCode == http.StatusNotFound && e.Code == "":
		return true
	}
	return false
}

// HasCode reports whether err is an [APIError] with the given code.
func HasCode(err error, code string) bool {
	apiErr, ok := errors.AsType[*APIError](err)
	return ok && apiErr.Code == code
}

func newAPIError(statusCode int, body []byte) *APIError {
	apiErr := &APIError{StatusCode: statusCode, Message: string(body)}
	var parsed struct {
		Code      string `json:"code"`
		ErrorCode string `json:"error_code"`
		Message   string `json:"message"`
	}
	if jsonrs.Unmarshal(body, &parsed) == nil {
		apiErr.Code = parsed.Code
		if apiErr.Code == "" {
			apiErr.Code = parsed.ErrorCode
		}
		if parsed.Message != "" {
			apiErr.Message = parsed.Message
		}
	}
	return apiErr
}

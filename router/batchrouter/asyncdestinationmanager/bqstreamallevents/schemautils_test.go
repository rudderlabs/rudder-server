package bqstreamallevents

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestFindNewColumns(t *testing.T) {
	tests := []struct {
		name           string
		eventSchema    whutils.ModelTableSchema
		snowpipeSchema whutils.ModelTableSchema
		expected       []whutils.ColumnInfo
	}{
		{
			name: "new column with different data type in event schema",
			eventSchema: whutils.ModelTableSchema{
				"new_column":      "STRING",
				"existing_column": "FLOAT",
			},
			snowpipeSchema: whutils.ModelTableSchema{
				"existing_column": "INT",
			},
			expected: []whutils.ColumnInfo{
				{Name: "new_column", Type: "STRING"},
			},
		},
		{
			name: "new and existing columns with multiple data types",
			eventSchema: whutils.ModelTableSchema{
				"new_column1":     "STRING",
				"new_column2":     "BOOLEAN",
				"existing_column": "INT",
			},
			snowpipeSchema: whutils.ModelTableSchema{
				"existing_column":         "INT",
				"another_existing_column": "FLOAT",
			},
			expected: []whutils.ColumnInfo{
				{Name: "new_column1", Type: "STRING"},
				{Name: "new_column2", Type: "BOOLEAN"},
			},
		},
		{
			name: "all columns in event schema are new",
			eventSchema: whutils.ModelTableSchema{
				"new_column1": "STRING",
				"new_column2": "BOOLEAN",
				"new_column3": "FLOAT",
			},
			snowpipeSchema: whutils.ModelTableSchema{},
			expected: []whutils.ColumnInfo{
				{Name: "new_column1", Type: "STRING"},
				{Name: "new_column2", Type: "BOOLEAN"},
				{Name: "new_column3", Type: "FLOAT"},
			},
		},
		{
			name: "case sensitivity check",
			eventSchema: whutils.ModelTableSchema{
				"ColumnA": "STRING",
				"columna": "BOOLEAN",
			},
			snowpipeSchema: whutils.ModelTableSchema{
				"columna": "BOOLEAN",
			},
			expected: []whutils.ColumnInfo{
				{Name: "ColumnA", Type: "STRING"},
			},
		},
		{
			name: "all columns match with identical types",
			eventSchema: whutils.ModelTableSchema{
				"existing_column1": "STRING",
				"existing_column2": "FLOAT",
			},
			snowpipeSchema: whutils.ModelTableSchema{
				"existing_column1": "STRING",
				"existing_column2": "FLOAT",
			},
			expected: []whutils.ColumnInfo{},
		},
		{
			name:        "event schema is empty, Snowpipe schema has columns",
			eventSchema: whutils.ModelTableSchema{},
			snowpipeSchema: whutils.ModelTableSchema{
				"existing_column": "STRING",
			},
			expected: []whutils.ColumnInfo{},
		},
		{
			name: "Snowpipe schema is nil",
			eventSchema: whutils.ModelTableSchema{
				"new_column": "STRING",
			},
			snowpipeSchema: nil,
			expected: []whutils.ColumnInfo{
				{Name: "new_column", Type: "STRING"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findNewColumns(tt.eventSchema, tt.snowpipeSchema)
			assert.ElementsMatch(t, tt.expected, result)
		})
	}
}

func TestCheckAndIgnoreAlreadyExistError(t *testing.T) {
	require.True(t, checkAndIgnoreAlreadyExistError(&googleapi.Error{Code: 409}))
	require.True(t, checkAndIgnoreAlreadyExistError(fmt.Errorf("creating table: %w", &googleapi.Error{Code: 400, Message: `column "x" already exists in schema`})))
	require.False(t, checkAndIgnoreAlreadyExistError(&googleapi.Error{Code: 400, Message: "bad request"}))
	require.False(t, checkAndIgnoreAlreadyExistError(errors.New("not a googleapi error")))
}

func TestShouldAbort(t *testing.T) {
	require.True(t, shouldAbort(status.Error(codes.PermissionDenied, "permission denied")))
	require.True(t, shouldAbort(status.Error(codes.Unauthenticated, "unauthenticated")))
	require.False(t, shouldAbort(status.Error(codes.Unavailable, "retryable")))
	require.False(t, shouldAbort(errors.New("plain error")))

	for _, tt := range []struct {
		name  string
		err   error
		abort bool
	}{
		{"terminal sentinel", fmt.Errorf("writer: %w", errTerminalAuth), true},
		{"invalid grant", &oauth2.RetrieveError{ErrorCode: "invalid_grant"}, true},
		{"JWT source invalid grant body", &oauth2.RetrieveError{Response: &http.Response{Status: "400 Bad Request"}, Body: []byte(`{"error":"invalid_grant","error_description":"Invalid JWT Signature."}`)}, true},
		{"JWT source transient body", &oauth2.RetrieveError{Response: &http.Response{Status: "503 Service Unavailable"}, Body: []byte(`{"error":"server_error","error_description":"cannot fetch token"}`)}, false},
		{"invalid client", &oauth2.RetrieveError{ErrorCode: "invalid_client"}, true},
		{"unauthorized client", &oauth2.RetrieveError{ErrorCode: "unauthorized_client"}, true},
		{"transient token response", &oauth2.RetrieveError{ErrorCode: "server_error"}, false},
		{"REST bad request", &googleapi.Error{Code: 400, Message: "bad request"}, true},
		{"REST unauthorized", &googleapi.Error{Code: 401}, true},
		{"REST forbidden", &googleapi.Error{Code: 403}, true},
		{"REST already exists", &googleapi.Error{Code: 400, Message: "already exists in schema"}, false},
		{"REST conflict", &googleapi.Error{Code: 409}, false},
		{"wrapped JWT signature from token fetch", fmt.Errorf("appending rows: %w", errors.New("oauth2: cannot fetch token: 400 Bad Request Response: Invalid JWT Signature")), true},
		{"flattened grant", status.Error(codes.Unavailable, "oauth2: cannot fetch token: invalid_grant"), true},
		{"flattened append credential failure", status.Error(codes.Unavailable, "transport: per-RPC creds failed due to error: oauth2: cannot fetch token: 400 Bad Request Response: Invalid JWT Signature"), true},
		{"table named invalid_grant", fmt.Errorf("adding columns to table invalid_grant: %w", errors.New("temporary metadata error")), false},
		{"REST table named invalid_grant", fmt.Errorf("adding columns to table invalid_grant: %w", &googleapi.Error{Code: 503, Message: "backend error"}), false},
		{"transient REST rate limit", &googleapi.Error{Code: 403, Errors: []googleapi.ErrorItem{{Reason: "rateLimitExceeded"}}}, false},
		{"transient REST user rate limit", &googleapi.Error{Code: 403, Errors: []googleapi.ErrorItem{{Reason: "userRateLimitExceeded"}}}, false},
		{"transient REST quota", &googleapi.Error{Code: 403, Errors: []googleapi.ErrorItem{{Reason: "quotaExceeded"}}}, false},
		{"transient REST backend error", &googleapi.Error{Code: 400, Errors: []googleapi.ErrorItem{{Reason: "backendError"}}}, false},
		{"concurrent query quota", &googleapi.Error{Code: 400, Message: "Job exceeded rate limits: Your project_and_region exceeded quota for concurrent queries."}, false},
		{"concurrent query rate limit", &googleapi.Error{Code: 400, Message: "Exceeded rate limits: too many concurrent queries for this project_and_region."}, false},
		{"transient 403 body mentions JWT", &googleapi.Error{Code: 403, Errors: []googleapi.ErrorItem{{Reason: "rateLimitExceeded"}}, Message: "table invalid_grant"}, false},
		{"flattened transient token response", status.Error(codes.Unavailable, "oauth2: cannot fetch token: 503 Service Unavailable"), false},
		{"transport transient token response", status.Error(codes.Unavailable, "transport: per-RPC creds failed due to error: oauth2: cannot fetch token: 503 Service Unavailable"), false},
		{"flattened transport token failure", status.Error(codes.Unavailable, "oauth2: cannot fetch token: network timeout"), false},
		{"joined terminal", errors.Join(errors.New("transient"), fmt.Errorf("writer: %w", errTerminalAuth)), true},
		{"joined REST terminal after already exists", errors.Join(&googleapi.Error{Code: 400, Message: "already exists in schema"}, &googleapi.Error{Code: 401}), true},
		{"joined OAuth terminal after transient", errors.Join(&oauth2.RetrieveError{ErrorCode: "server_error"}, &oauth2.RetrieveError{ErrorCode: "invalid_grant"}), true},
		{"wrapped joined terminal after transient", fmt.Errorf("writing: %w", errors.Join(status.Error(codes.Unavailable, "retryable"), &googleapi.Error{Code: 403})), true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.abort, shouldAbort(tt.err))
		})
	}
}

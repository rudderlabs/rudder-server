package bqstreamallevents

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bigqueryintegration "github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery"
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
	tokenError := func(body string) error {
		return fmt.Errorf("fetching schema: %w", &url.Error{Op: "Post", Err: &auth.Error{Response: &http.Response{StatusCode: http.StatusBadRequest}, Body: []byte(body)}})
	}
	for _, tt := range []struct {
		name  string
		err   error
		abort bool
	}{
		{"permission denied", status.Error(codes.PermissionDenied, "permission denied"), true},
		{"unauthenticated", status.Error(codes.Unauthenticated, "unauthenticated"), true},
		{"unavailable", status.Error(codes.Unavailable, "retryable"), false},
		{"plain error", errors.New("plain error"), false},
		{"token invalid grant", tokenError(`{"error":"invalid_grant","error_description":"Invalid grant: account not found"}`), true},
		{"token invalid JWT signature", tokenError(`{"error":"invalid_grant","error_description":"Invalid JWT Signature."}`), true},
		{"token clock skew", tokenError(`{"error":"invalid_grant","error_description":"Invalid JWT: Token must be a short-lived token (60 minutes) and in a reasonable timeframe. Check your iat and exp values in the JWT claim."}`), false},
		{"token invalid client", tokenError(`{"error":"invalid_client"}`), true},
		{"token unauthorized client", tokenError(`{"error":"unauthorized_client"}`), true},
		{"token server error", tokenError(`{"error":"server_error"}`), false},
		{"token non-JSON body", tokenError(`<html>503</html>`), false},
		{"malformed private key", fmt.Errorf("fetching schema: %w", &url.Error{Op: "Post", Err: malformedKeyError(t)}), true},
		{"incompatible credentials", fmt.Errorf("setting up bigquery manager: %w", fmt.Errorf("%w: %w", bigqueryintegration.ErrIncompatibleCredentials, errors.New("unsupported credential type"))), true},
		{"event value naming credential errors", errors.Join(fmt.Errorf(`parsing datetime string "incompatible credentials: failed to parse private key"`), status.Error(codes.Unavailable, "retryable")), false},
		{"REST unauthorized", &googleapi.Error{Code: 401}, true},
		{"REST forbidden", &googleapi.Error{Code: 403, Errors: []googleapi.ErrorItem{{Reason: "accessDenied"}}}, true},
		{"REST rate limit", &googleapi.Error{Code: 403, Errors: []googleapi.ErrorItem{{Reason: "rateLimitExceeded"}}}, false},
		{"REST user rate limit", &googleapi.Error{Code: 403, Errors: []googleapi.ErrorItem{{Reason: "userRateLimitExceeded"}}}, false},
		{"REST quota", &googleapi.Error{Code: 403, Errors: []googleapi.ErrorItem{{Reason: "quotaExceeded"}}}, false},
		{"REST bad request", &googleapi.Error{Code: 400, Message: "bad request"}, false},
		{"REST conflict", &googleapi.Error{Code: 409}, false},
		{"joined REST terminal after conflict", errors.Join(&googleapi.Error{Code: 409}, &googleapi.Error{Code: 401}), true},
		{"wrapped joined terminal after transient", fmt.Errorf("writing: %w", errors.Join(status.Error(codes.Unavailable, "retryable"), &googleapi.Error{Code: 403})), true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.abort, shouldAbort(tt.err))
		})
	}
}

// malformedKeyError returns the auth library's own error for an unparseable
// service-account key, so a rewording upstream fails this test.
func malformedKeyError(t *testing.T) error {
	t.Helper()
	creds, err := credentials.NewCredentialsFromJSON(credentials.ServiceAccount,
		[]byte(`{"type":"service_account","client_email":"test@example.com","private_key":"invalid","token_uri":"http://127.0.0.1:1"}`),
		&credentials.DetectOptions{Scopes: []string{"https://www.googleapis.com/auth/bigquery"}},
	)
	require.NoError(t, err)
	_, err = creds.Token(context.Background())
	require.Error(t, err)
	return err
}

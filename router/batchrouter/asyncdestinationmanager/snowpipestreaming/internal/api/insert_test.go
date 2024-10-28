package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/api"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
)

func TestInsert(t *testing.T) {
	successChannelID := "successChannelID"
	failureChannelID := "failureChannelID"
	ir := &model.InsertRequest{Rows: []model.Row{{"key1": "value1"}, {"key2": "value2"}}, Offset: "5"}

	snowPipeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))

		irJSON, err := json.Marshal(ir)
		require.NoError(t, err)

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())
		require.JSONEq(t, string(irJSON), string(body))

		switch r.URL.String() {
		case "/channels/" + successChannelID + "/insert":
			_, err := w.Write([]byte(`{"success":true}`))
			require.NoError(t, err)
		case "/channels/" + failureChannelID + "/insert":
			_, err := w.Write([]byte(`{"success":false,"errors":[{"message":"The given row cannot be converted to the internal format due to invalid value: Value cannot be ingested into Snowflake column RECEIVED_AT of type TIMESTAMP, rowIndex:0, reason: Not a valid value, see https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview for the list of supported formats","rowIndex":0,"missingNotNullColNames":null,"nullValueForNotNullColNames":null,"extraColNames":null},{"message":"The given row cannot be converted to the internal format: Extra columns: [UNKNOWN]. Columns not present in the table shouldn't be specified, rowIndex:1","rowIndex":1,"missingNotNullColNames":null,"nullValueForNotNullColNames":null,"extraColNames":["UNKNOWN"]}],"code":"ERR_SCHEMA_CONFLICT"}`))
			require.NoError(t, err)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer snowPipeServer.Close()

	ctx := context.Background()

	t.Run("Insert success", func(t *testing.T) {
		manager := api.New(snowPipeServer.URL, snowPipeServer.Client())
		res, err := manager.Insert(ctx, successChannelID, ir)
		require.NoError(t, err)
		require.Equal(t, &model.InsertResponse{Success: true, Errors: nil}, res)
	})
	t.Run("Insert failure", func(t *testing.T) {
		manager := api.New(snowPipeServer.URL, snowPipeServer.Client())
		res, err := manager.Insert(ctx, failureChannelID, ir)
		require.NoError(t, err)
		require.Equal(t, &model.InsertResponse{
			Success: false,
			Errors: []model.InsertError{
				{
					RowIndex:                    0,
					ExtraColNames:               nil,
					MissingNotNullColNames:      nil,
					NullValueForNotNullColNames: nil,
					Message:                     "The given row cannot be converted to the internal format due to invalid value: Value cannot be ingested into Snowflake column RECEIVED_AT of type TIMESTAMP, rowIndex:0, reason: Not a valid value, see https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview for the list of supported formats",
				},
				{
					RowIndex:                    1,
					ExtraColNames:               []string{"UNKNOWN"},
					NullValueForNotNullColNames: nil,
					Message:                     "The given row cannot be converted to the internal format: Extra columns: [UNKNOWN]. Columns not present in the table shouldn't be specified, rowIndex:1",
				},
			},
			Code: "ERR_SCHEMA_CONFLICT",
		},
			res,
		)
	})
	t.Run("Request failure", func(t *testing.T) {
		manager := api.New(snowPipeServer.URL, &mockRequestDoer{
			err: errors.New("bad client"),
			response: &http.Response{
				StatusCode: http.StatusOK,
			},
		})
		res, err := manager.Insert(ctx, successChannelID, ir)
		require.Error(t, err)
		require.Nil(t, res)
	})
	t.Run("Request failure (non 200's status code)", func(t *testing.T) {
		manager := api.New(snowPipeServer.URL, &mockRequestDoer{
			response: &http.Response{
				StatusCode: http.StatusBadRequest,
				Body:       nopReadCloser{Reader: bytes.NewReader([]byte(`{}`))},
			},
		})
		res, err := manager.Insert(ctx, successChannelID, ir)
		require.Error(t, err)
		require.Nil(t, res)
	})
	t.Run("Request failure (invalid response)", func(t *testing.T) {
		manager := api.New(snowPipeServer.URL, &mockRequestDoer{
			response: &http.Response{
				StatusCode: http.StatusOK,
				Body:       nopReadCloser{Reader: bytes.NewReader([]byte(`{abd}`))},
			},
		})
		res, err := manager.Insert(ctx, successChannelID, ir)
		require.Error(t, err)
		require.Nil(t, res)
	})
}

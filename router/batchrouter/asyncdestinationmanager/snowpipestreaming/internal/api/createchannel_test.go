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
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestCreateChannel(t *testing.T) {
	ccr := &model.CreateChannelRequest{
		RudderIdentifier: "rudderIdentifier",
		Partition:        "partition",
		AccountConfig: model.AccountConfig{
			Account:              "account",
			User:                 "user",
			Role:                 "role",
			PrivateKey:           "privateKey",
			PrivateKeyPassphrase: "privateKeyPassphrase",
		},
		TableConfig: model.TableConfig{
			Database: "database",
			Schema:   "schema",
			Table:    "table",
		},
	}

	successSnowPipeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))

		ccrJSON, err := json.Marshal(ccr)
		require.NoError(t, err)

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())
		require.JSONEq(t, string(ccrJSON), string(body))

		switch r.URL.String() {
		case "/channels":
			_, err := w.Write([]byte(`{"success": true,"channelId":"channelId","channelName":"channelName","clientName":"clientName","valid":true,"deleted":false,"tableSchema":{"EVENT":{"type":"VARCHAR(16777216)","logicalType":"TEXT","precision":null,"scale":null,"byteLength":16777216,"length":16777216,"nullable":true},"ID":{"type":"VARCHAR(16777216)","logicalType":"TEXT","precision":null,"scale":null,"byteLength":16777216,"length":16777216,"nullable":true},"TIMESTAMP":{"type":"TIMESTAMP_TZ(9)","logicalType":"TIMESTAMP_TZ","precision":0,"scale":9,"byteLength":null,"length":null,"nullable":true}}}`))
			require.NoError(t, err)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer successSnowPipeServer.Close()

	failureSnowPipeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))

		ccrJSON, err := json.Marshal(ccr)
		require.NoError(t, err)

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())
		require.JSONEq(t, string(ccrJSON), string(body))

		switch r.URL.String() {
		case "/channels":
			_, err := w.Write([]byte(`{"success":false,"error":"Open channel request failed: HTTP Status: 400 ErrorBody: {\n  \"status_code\" : 4,\n  \"message\" : \"The supplied table does not exist or is not authorized.\"\n}.","code":"ERR_TABLE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED","snowflakeSDKCode":"0007","snowflakeAPIHttpCode":400,"snowflakeAPIStatusCode":4,"snowflakeAPIMessage":"The supplied table does not exist or is not authorized."}`))
			require.NoError(t, err)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer failureSnowPipeServer.Close()

	ctx := context.Background()

	t.Run("Status=200(success=true)", func(t *testing.T) {
		manager := api.New(successSnowPipeServer.URL, successSnowPipeServer.Client())
		res, err := manager.CreateChannel(ctx, ccr)
		require.NoError(t, err)
		require.EqualValues(t, &model.ChannelResponse{
			Success:        true,
			ChannelID:      "channelId",
			ChannelName:    "channelName",
			ClientName:     "clientName",
			Valid:          true,
			Deleted:        false,
			SnowPipeSchema: whutils.ModelTableSchema{"EVENT": "string", "ID": "string", "TIMESTAMP": "datetime"},
		},
			res,
		)
	})
	t.Run("Status=200(success=false)", func(t *testing.T) {
		manager := api.New(failureSnowPipeServer.URL, failureSnowPipeServer.Client())
		res, err := manager.CreateChannel(ctx, ccr)
		require.NoError(t, err)
		require.EqualValues(t, &model.ChannelResponse{
			Success:                false,
			Error:                  "Open channel request failed: HTTP Status: 400 ErrorBody: {\n  \"status_code\" : 4,\n  \"message\" : \"The supplied table does not exist or is not authorized.\"\n}.",
			Code:                   "ERR_TABLE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED",
			SnowflakeSDKCode:       "0007",
			SnowflakeAPIHttpCode:   400,
			SnowflakeAPIStatusCode: 4,
			SnowflakeAPIMessage:    "The supplied table does not exist or is not authorized.",
		},
			res,
		)
	})
	t.Run("Request failure", func(t *testing.T) {
		manager := api.New(successSnowPipeServer.URL, &mockRequestDoer{
			err: errors.New("bad client"),
		})
		res, err := manager.CreateChannel(ctx, ccr)
		require.Error(t, err)
		require.Nil(t, res)
	})
	t.Run("Request failure (non 200's status code)", func(t *testing.T) {
		manager := api.New(successSnowPipeServer.URL, &mockRequestDoer{
			response: &http.Response{
				StatusCode: http.StatusBadRequest,
				Body:       nopReadCloser{Reader: bytes.NewReader([]byte(`{}`))},
			},
		})
		res, err := manager.CreateChannel(ctx, ccr)
		require.Error(t, err)
		require.Nil(t, res)
	})
	t.Run("Request failure (invalid response)", func(t *testing.T) {
		manager := api.New(successSnowPipeServer.URL, &mockRequestDoer{
			response: &http.Response{
				StatusCode: http.StatusOK,
				Body:       nopReadCloser{Reader: bytes.NewReader([]byte(`{abd}`))},
			},
		})
		res, err := manager.CreateChannel(ctx, ccr)
		require.Error(t, err)
		require.Nil(t, res)
	})
}

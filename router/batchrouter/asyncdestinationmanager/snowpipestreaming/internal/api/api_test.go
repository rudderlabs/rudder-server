package api_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/api"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type mockRequestDoer struct {
	response *http.Response
	err      error
}

func (c *mockRequestDoer) Do(*http.Request) (*http.Response, error) {
	return c.response, c.err
}

type nopReadCloser struct {
	io.Reader
}

func (nopReadCloser) Close() error {
	return nil
}

func TestAPI(t *testing.T) {
	channelID := "channelId"

	t.Run("Create Channel", func(t *testing.T) {
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

		successSnowpipeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		defer successSnowpipeServer.Close()

		failureSnowpipeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		defer failureSnowpipeServer.Close()

		ctx := context.Background()

		t.Run("Status=200(success=true)", func(t *testing.T) {
			manager := api.New(config.New(), stats.NOP, successSnowpipeServer.URL, successSnowpipeServer.Client())
			res, err := manager.CreateChannel(ctx, ccr)
			require.NoError(t, err)
			require.EqualValues(t, &model.ChannelResponse{
				Success:        true,
				ChannelID:      "channelId",
				ChannelName:    "channelName",
				ClientName:     "clientName",
				Valid:          true,
				Deleted:        false,
				SnowpipeSchema: whutils.ModelTableSchema{"EVENT": "string", "ID": "string", "TIMESTAMP": "datetime"},
			},
				res,
			)
		})
		t.Run("Status=200(success=false)", func(t *testing.T) {
			manager := api.New(config.New(), stats.NOP, failureSnowpipeServer.URL, failureSnowpipeServer.Client())
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
			manager := api.New(config.New(), stats.NOP, successSnowpipeServer.URL, &mockRequestDoer{
				err: errors.New("bad client"),
			})
			res, err := manager.CreateChannel(ctx, ccr)
			require.Error(t, err)
			require.Nil(t, res)
		})
		t.Run("Request failure (non 200's status code)", func(t *testing.T) {
			manager := api.New(config.New(), stats.NOP, successSnowpipeServer.URL, &mockRequestDoer{
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
			manager := api.New(config.New(), stats.NOP, successSnowpipeServer.URL, &mockRequestDoer{
				response: &http.Response{
					StatusCode: http.StatusOK,
					Body:       nopReadCloser{Reader: bytes.NewReader([]byte(`{abd}`))},
				},
			})
			res, err := manager.CreateChannel(ctx, ccr)
			require.Error(t, err)
			require.Nil(t, res)
		})
	})
	t.Run("Delete Channel", func(t *testing.T) {
		snowpipeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodDelete, r.Method)
			require.Equal(t, "application/json", r.Header.Get("Content-Type"))

			switch r.URL.String() {
			case "/channels/" + channelID + "?sync=true":
				w.WriteHeader(http.StatusNoContent)
			case "/channels/" + channelID + "?sync=false":
				w.WriteHeader(http.StatusAccepted)
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer snowpipeServer.Close()

		ctx := context.Background()

		t.Run("Success", func(t *testing.T) {
			t.Run("sync=true", func(t *testing.T) {
				manager := api.New(config.New(), stats.NOP, snowpipeServer.URL, snowpipeServer.Client())
				err := manager.DeleteChannel(ctx, channelID, true)
				require.NoError(t, err)
			})
			t.Run("sync=false", func(t *testing.T) {
				manager := api.New(config.New(), stats.NOP, snowpipeServer.URL, snowpipeServer.Client())
				err := manager.DeleteChannel(ctx, channelID, false)
				require.NoError(t, err)
			})
		})
		t.Run("Request failure", func(t *testing.T) {
			manager := api.New(config.New(), stats.NOP, snowpipeServer.URL, &mockRequestDoer{
				err: errors.New("bad client"),
			})
			err := manager.DeleteChannel(ctx, channelID, true)
			require.Error(t, err)
		})
		t.Run("Request failure (non 200's status code)", func(t *testing.T) {
			manager := api.New(config.New(), stats.NOP, snowpipeServer.URL, &mockRequestDoer{
				response: &http.Response{
					StatusCode: http.StatusBadRequest,
					Body:       nopReadCloser{Reader: bytes.NewReader([]byte(`{}`))},
				},
			})
			err := manager.DeleteChannel(ctx, channelID, true)
			require.Error(t, err)
		})
	})
	t.Run("Get Channel", func(t *testing.T) {
		snowpipeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodGet, r.Method)
			require.Equal(t, "application/json", r.Header.Get("Content-Type"))

			switch r.URL.String() {
			case "/channels/" + channelID:
				_, err := w.Write([]byte(`{"channelId":"channelId","channelName":"channelName","clientName":"clientName","valid":true,"deleted":false,"tableSchema":{"EVENT":{"type":"VARCHAR(16777216)","logicalType":"TEXT","precision":null,"scale":null,"byteLength":16777216,"length":16777216,"nullable":true},"ID":{"type":"VARCHAR(16777216)","logicalType":"TEXT","precision":null,"scale":null,"byteLength":16777216,"length":16777216,"nullable":true},"TIMESTAMP":{"type":"TIMESTAMP_TZ(9)","logicalType":"TIMESTAMP_TZ","precision":0,"scale":9,"byteLength":null,"length":null,"nullable":true}}}`))
				require.NoError(t, err)
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer snowpipeServer.Close()

		ctx := context.Background()

		t.Run("Success", func(t *testing.T) {
			manager := api.New(config.New(), stats.NOP, snowpipeServer.URL, snowpipeServer.Client())
			res, err := manager.GetChannel(ctx, channelID)
			require.NoError(t, err)
			require.EqualValues(t, &model.ChannelResponse{
				ChannelID:      "channelId",
				ChannelName:    "channelName",
				ClientName:     "clientName",
				Valid:          true,
				Deleted:        false,
				SnowpipeSchema: whutils.ModelTableSchema{"EVENT": "string", "ID": "string", "TIMESTAMP": "datetime"},
			},
				res,
			)
		})
		t.Run("Request failure", func(t *testing.T) {
			manager := api.New(config.New(), stats.NOP, snowpipeServer.URL, &mockRequestDoer{
				err: errors.New("bad client"),
			})
			res, err := manager.GetChannel(ctx, channelID)
			require.Error(t, err)
			require.Nil(t, res)
		})
		t.Run("Request failure (non 200's status code)", func(t *testing.T) {
			manager := api.New(config.New(), stats.NOP, snowpipeServer.URL, &mockRequestDoer{
				response: &http.Response{
					StatusCode: http.StatusBadRequest,
					Body:       nopReadCloser{Reader: bytes.NewReader([]byte(`{}`))},
				},
			})
			res, err := manager.GetChannel(ctx, channelID)
			require.Error(t, err)
			require.Nil(t, res)
		})
		t.Run("Request failure (invalid response)", func(t *testing.T) {
			manager := api.New(config.New(), stats.NOP, snowpipeServer.URL, &mockRequestDoer{
				response: &http.Response{
					StatusCode: http.StatusOK,
					Body:       nopReadCloser{Reader: bytes.NewReader([]byte(`{abd}`))},
				},
			})
			res, err := manager.GetChannel(ctx, channelID)
			require.Error(t, err)
			require.Nil(t, res)
		})
	})
	t.Run("insert", func(t *testing.T) {
		successChannelID := "successChannelID"
		failureChannelID := "failureChannelID"
		ir := &model.InsertRequest{Rows: []model.Row{{"key1": "value1"}, {"key2": "value2"}}, Offset: "5"}

		snowpipeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodPost, r.Method)
			require.Equal(t, "application/json", r.Header.Get("Content-Type"))

			irJSON, err := json.Marshal(ir)
			require.NoError(t, err)

			gz, err := gzip.NewReader(r.Body)
			require.NoError(t, err)

			body, err := io.ReadAll(gz)
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
		defer snowpipeServer.Close()

		ctx := context.Background()

		t.Run("Insert success", func(t *testing.T) {
			manager := api.New(config.New(), stats.NOP, snowpipeServer.URL, snowpipeServer.Client())
			res, err := manager.Insert(ctx, successChannelID, ir)
			require.NoError(t, err)
			require.Equal(t, &model.InsertResponse{Success: true, Errors: nil}, res)
		})
		t.Run("Insert failure", func(t *testing.T) {
			manager := api.New(config.New(), stats.NOP, snowpipeServer.URL, snowpipeServer.Client())
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
			manager := api.New(config.New(), stats.NOP, snowpipeServer.URL, &mockRequestDoer{
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
			manager := api.New(config.New(), stats.NOP, snowpipeServer.URL, &mockRequestDoer{
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
			manager := api.New(config.New(), stats.NOP, snowpipeServer.URL, &mockRequestDoer{
				response: &http.Response{
					StatusCode: http.StatusOK,
					Body:       nopReadCloser{Reader: bytes.NewReader([]byte(`{abd}`))},
				},
			})
			res, err := manager.Insert(ctx, successChannelID, ir)
			require.Error(t, err)
			require.Nil(t, res)
		})
	})
	t.Run("Get Statu", func(t *testing.T) {
		snowpipeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodGet, r.Method)
			require.Equal(t, "application/json", r.Header.Get("Content-Type"))

			switch r.URL.String() {
			case "/channels/" + channelID + "/status":
				_, err := w.Write([]byte(`{"success": true, "offset":"5","valid":true}`))
				require.NoError(t, err)
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer snowpipeServer.Close()

		ctx := context.Background()

		t.Run("Success", func(t *testing.T) {
			manager := api.New(config.New(), stats.NOP, snowpipeServer.URL, snowpipeServer.Client())
			res, err := manager.GetStatus(ctx, channelID)
			require.NoError(t, err)
			require.Equal(t, &model.StatusResponse{
				Success: true,
				Offset:  "5",
				Valid:   true,
			},
				res,
			)
		})
		t.Run("Request failure", func(t *testing.T) {
			manager := api.New(config.New(), stats.NOP, snowpipeServer.URL, &mockRequestDoer{
				err: errors.New("bad client"),
				response: &http.Response{
					StatusCode: http.StatusOK,
				},
			})
			res, err := manager.GetStatus(ctx, channelID)
			require.Error(t, err)
			require.Nil(t, res)
		})
		t.Run("Request failure (non 200's status code)", func(t *testing.T) {
			manager := api.New(config.New(), stats.NOP, snowpipeServer.URL, &mockRequestDoer{
				response: &http.Response{
					StatusCode: http.StatusBadRequest,
					Body:       nopReadCloser{Reader: bytes.NewReader([]byte(`{}`))},
				},
			})
			res, err := manager.GetStatus(ctx, channelID)
			require.Error(t, err)
			require.Nil(t, res)
		})
		t.Run("Request failure (invalid response)", func(t *testing.T) {
			manager := api.New(config.New(), stats.NOP, snowpipeServer.URL, &mockRequestDoer{
				response: &http.Response{
					StatusCode: http.StatusOK,
					Body:       nopReadCloser{Reader: bytes.NewReader([]byte(`{abd}`))},
				},
			})
			res, err := manager.GetStatus(ctx, channelID)
			require.Error(t, err)
			require.Nil(t, res)
		})
	})
}

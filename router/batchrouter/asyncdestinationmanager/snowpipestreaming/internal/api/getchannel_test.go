package api

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
)

func TestGetChannel(t *testing.T) {
	channelID := "channelId"

	snowPipeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	defer snowPipeServer.Close()

	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		manager := New(snowPipeServer.URL, snowPipeServer.Client())
		res, err := manager.GetChannel(ctx, channelID)
		require.NoError(t, err)
		require.EqualValues(t, &model.ChannelResponse{
			ChannelID:   "channelId",
			ChannelName: "channelName",
			ClientName:  "clientName",
			Valid:       true,
			Deleted:     false,
			TableSchema: map[string]map[string]interface{}{
				"EVENT": {
					"byteLength":  1.6777216e+07,
					"length":      1.6777216e+07,
					"logicalType": "TEXT",
					"nullable":    true,
					"precision":   nil,
					"scale":       nil,
					"type":        "VARCHAR(16777216)",
				},
				"ID": {
					"byteLength":  1.6777216e+07,
					"length":      1.6777216e+07,
					"logicalType": "TEXT",
					"nullable":    true,
					"precision":   nil,
					"scale":       nil,
					"type":        "VARCHAR(16777216)",
				},
				"TIMESTAMP": {
					"byteLength":  nil,
					"length":      nil,
					"logicalType": "TIMESTAMP_TZ",
					"nullable":    true,
					"precision":   float64(0),
					"scale":       float64(9),
					"type":        "TIMESTAMP_TZ(9)",
				},
			},
		},
			res,
		)
	})
	t.Run("Request failure", func(t *testing.T) {
		manager := New(snowPipeServer.URL, &mockRequestDoer{
			err: errors.New("bad client"),
		})
		res, err := manager.GetChannel(ctx, channelID)
		require.Error(t, err)
		require.Nil(t, res)
	})
	t.Run("Request failure (non 200's status code)", func(t *testing.T) {
		manager := New(snowPipeServer.URL, &mockRequestDoer{
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
		manager := New(snowPipeServer.URL, &mockRequestDoer{
			response: &http.Response{
				StatusCode: http.StatusOK,
				Body:       nopReadCloser{Reader: bytes.NewReader([]byte(`{abd}`))},
			},
		})
		res, err := manager.GetChannel(ctx, channelID)
		require.Error(t, err)
		require.Nil(t, res)
	})
}

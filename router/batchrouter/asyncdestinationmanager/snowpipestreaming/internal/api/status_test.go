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

func TestStatus(t *testing.T) {
	channelID := "channelID"

	snowPipeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	defer snowPipeServer.Close()

	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		manager := New(snowPipeServer.URL, snowPipeServer.Client())
		res, err := manager.Status(ctx, channelID)
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
		manager := New(snowPipeServer.URL, &mockRequestDoer{
			err: errors.New("bad client"),
			response: &http.Response{
				StatusCode: http.StatusOK,
			},
		})
		res, err := manager.Status(ctx, channelID)
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
		res, err := manager.Status(ctx, channelID)
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
		res, err := manager.Status(ctx, channelID)
		require.Error(t, err)
		require.Nil(t, res)
	})
}

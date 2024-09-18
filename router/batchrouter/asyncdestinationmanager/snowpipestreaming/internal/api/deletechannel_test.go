package api

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeleteChannel(t *testing.T) {
	channelID := "channelID"

	snowPipeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	defer snowPipeServer.Close()

	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		t.Run("sync=true", func(t *testing.T) {
			manager := New(snowPipeServer.URL, snowPipeServer.Client())
			err := manager.DeleteChannel(ctx, channelID, true)
			require.NoError(t, err)
		})
		t.Run("sync=false", func(t *testing.T) {
			manager := New(snowPipeServer.URL, snowPipeServer.Client())
			err := manager.DeleteChannel(ctx, channelID, false)
			require.NoError(t, err)
		})
	})
	t.Run("Request failure", func(t *testing.T) {
		manager := New(snowPipeServer.URL, &mockRequestDoer{
			err: errors.New("bad client"),
		})
		err := manager.DeleteChannel(ctx, channelID, true)
		require.Error(t, err)
	})
	t.Run("Request failure (non 200's status code)", func(t *testing.T) {
		manager := New(snowPipeServer.URL, &mockRequestDoer{
			response: &http.Response{
				StatusCode: http.StatusBadRequest,
				Body:       nopReadCloser{Reader: bytes.NewReader([]byte(`{}`))},
			},
		})
		err := manager.DeleteChannel(ctx, channelID, true)
		require.Error(t, err)
	})
}

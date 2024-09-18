package snowpipestreaming

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
)

func TestStatus(t *testing.T) {
	channelID := "channelID"

	snowpipeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())
		require.Empty(t, body)

		switch r.URL.String() {
		case "/channels/" + channelID + "/status":
			_, err := w.Write([]byte(`{"offset":"5","valid":true}`))
			require.NoError(t, err)
		default:
			require.FailNowf(t, "SnowpipeClients", "Unexpected %s to SnowpipeClients, not found: %+v", r.Method, r.URL)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer snowpipeServer.Close()

	t.Run("Success", func(t *testing.T) {
		ctx := context.Background()
		destination := backendconfigtest.
			NewDestinationBuilder("SNOWPIPE_STREAMING").
			Build()

		c := config.New()
		c.Set("Snowpipe.Client.URL", snowpipeServer.URL)

		manager := New(c, logger.NOP, stats.NOP, &destination, WithRequestDoer(snowpipeServer.Client()))
		res, err := manager.status(ctx, channelID)
		require.NoError(t, err)
		require.Equal(t, "5", res.Offset)
		require.True(t, res.Valid)
	})
	t.Run("Request failure", func(t *testing.T) {
		ctx := context.Background()
		destination := backendconfigtest.
			NewDestinationBuilder("SNOWPIPE_STREAMING").
			Build()

		c := config.New()
		c.Set("Snowpipe.Client.URL", snowpipeServer.URL)

		reqDoer := &mockRequestDoer{
			err: errors.New("bad client"),
			response: &http.Response{
				StatusCode: http.StatusOK,
			},
		}

		manager := New(c, logger.NOP, stats.NOP, &destination, WithRequestDoer(reqDoer))
		res, err := manager.status(ctx, channelID)
		require.Error(t, err)
		require.Nil(t, res)
	})
	t.Run("Request failure (non 200's status code)", func(t *testing.T) {
		ctx := context.Background()
		destination := backendconfigtest.
			NewDestinationBuilder("SNOWPIPE_STREAMING").
			Build()

		c := config.New()
		c.Set("Snowpipe.Client.URL", snowpipeServer.URL)

		reqDoer := &mockRequestDoer{
			response: &http.Response{
				StatusCode: http.StatusBadRequest,
				Body:       nopReadCloser{Reader: bytes.NewReader([]byte(`{}`))},
			},
		}

		manager := New(c, logger.NOP, stats.NOP, &destination, WithRequestDoer(reqDoer))
		res, err := manager.status(ctx, channelID)
		require.Error(t, err)
		require.Nil(t, res)
	})
	t.Run("Request failure (invalid response)", func(t *testing.T) {
		ctx := context.Background()
		destination := backendconfigtest.
			NewDestinationBuilder("SNOWPIPE_STREAMING").
			Build()

		c := config.New()
		c.Set("Snowpipe.Client.URL", snowpipeServer.URL)

		reqDoer := &mockRequestDoer{
			response: &http.Response{
				StatusCode: http.StatusOK,
				Body:       nopReadCloser{Reader: bytes.NewReader([]byte(`{abd}`))},
			},
		}

		manager := New(c, logger.NOP, stats.NOP, &destination, WithRequestDoer(reqDoer))
		res, err := manager.status(ctx, channelID)
		require.Error(t, err)
		require.Nil(t, res)
	})
}

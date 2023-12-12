package gateway

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/gateway/response"
)

func TestPixelInterceptor(t *testing.T) {
	refTime := time.Now().UTC()

	newGateway := func() *Handle {
		return &Handle{
			logger: logger.NOP,
			stats:  stats.Default,
			now:    func() time.Time { return refTime },
		}
	}
	newPixelRequest := func(values url.Values) *http.Request {
		r := httptest.NewRequest("GET", "/", nil)
		if values != nil {
			r.URL.RawQuery = values.Encode()
		}
		return r
	}

	t.Run("valid request without extra fields", func(t *testing.T) {
		var payload []byte
		delegate := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			payload, _ = io.ReadAll(r.Body)
			_, _ = w.Write([]byte("OK"))
			w.WriteHeader(http.StatusOK)
		})
		gw := newGateway()
		r := newPixelRequest(url.Values{
			"writeKey": []string{"123"},
		})
		w := httptest.NewRecorder()
		gw.pixelInterceptor("pixel", delegate).ServeHTTP(w, r)

		require.Equal(t, http.StatusOK, w.Code, "request should succeed")
		gif, err := io.ReadAll(w.Body)
		require.NoError(t, err, "reading response body should succeed")
		require.Equal(t, response.GetPixelResponse(), string(gif))

		require.NotNil(t, payload)
		require.Equal(t, fmt.Sprintf(`{"channel": "web","integrations": {"All": true},"originalTimestamp":"%[1]s","sentAt":"%[1]s","type":"pixel"}`, refTime.Format(time.RFC3339Nano)), string(payload))
	})

	t.Run("valid request with extra fields", func(t *testing.T) {
		var payload []byte
		delegate := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			payload, _ = io.ReadAll(r.Body)
			_, _ = w.Write([]byte("OK"))
		})
		gw := newGateway()
		r := newPixelRequest(url.Values{
			"writeKey": []string{"123"},
			"random":   []string{"random"},
		})
		w := httptest.NewRecorder()
		gw.pixelInterceptor("page", delegate).ServeHTTP(w, r)

		require.Equal(t, http.StatusOK, w.Code, "request should succeed")
		gif, err := io.ReadAll(w.Body)
		require.NoError(t, err, "reading response body should succeed")
		require.Equal(t, response.GetPixelResponse(), string(gif))

		require.NotNil(t, payload)
		require.Equal(t, fmt.Sprintf(`{"channel": "web","integrations": {"All": true},"originalTimestamp":"%[1]s","sentAt":"%[1]s","random":"random","type":"page"}`, refTime.Format(time.RFC3339Nano)), string(payload))
	})

	t.Run("no writeKey", func(t *testing.T) {
		var payload []byte
		delegate := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			payload, _ = io.ReadAll(r.Body)
			_, _ = w.Write([]byte("OK"))
		})
		gw := newGateway()
		r := newPixelRequest(url.Values{})
		w := httptest.NewRecorder()
		gw.pixelInterceptor("pixel", delegate).ServeHTTP(w, r)

		require.Equal(t, http.StatusOK, w.Code, "request should succeed")
		gif, err := io.ReadAll(w.Body)
		require.NoError(t, err, "reading response body should succeed")
		require.Equal(t, response.GetPixelResponse(), string(gif))

		require.Nil(t, payload, "request should not have been forwarded")
	})

	t.Run("track without event", func(t *testing.T) {
		var payload []byte
		delegate := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			payload, _ = io.ReadAll(r.Body)
			_, _ = w.Write([]byte("OK"))
		})
		gw := newGateway()
		r := newPixelRequest(url.Values{
			"writeKey": []string{"123"},
			"event":    []string{""},
		})
		w := httptest.NewRecorder()
		gw.pixelInterceptor("track", delegate).ServeHTTP(w, r)

		require.Equal(t, http.StatusOK, w.Code, "request should succeed")
		gif, err := io.ReadAll(w.Body)
		require.NoError(t, err, "reading response body should succeed")
		require.Equal(t, response.GetPixelResponse(), string(gif))

		require.Nil(t, payload, "request should not have been forwarded")
	})
}

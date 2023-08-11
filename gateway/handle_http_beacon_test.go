package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

func TestBeaconInterceptor(t *testing.T) {
	refTime := time.Now().UTC()

	delegate := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})

	newGateway := func() *Handle {
		return &Handle{
			logger: logger.NOP,
			stats:  stats.Default,
			now:    func() time.Time { return refTime },
		}
	}
	newBeaconRequest := func(values url.Values) *http.Request {
		r := httptest.NewRequest("GET", "/", nil)
		if values != nil {
			r.URL.RawQuery = values.Encode()
		}
		return r
	}

	t.Run("valid request", func(t *testing.T) {
		gw := newGateway()
		r := newBeaconRequest(url.Values{
			"writeKey": []string{"123"},
		})
		w := httptest.NewRecorder()
		gw.beaconInterceptor(delegate).ServeHTTP(w, r)

		require.Equal(t, http.StatusOK, w.Code, "request should succeed")
		gif, err := io.ReadAll(w.Body)
		require.NoError(t, err, "reading response body should succeed")
		require.Equal(t, "OK", string(gif))
	})

	t.Run("no writeKey", func(t *testing.T) {
		gw := newGateway()
		r := newBeaconRequest(url.Values{})
		w := httptest.NewRecorder()
		gw.beaconInterceptor(delegate).ServeHTTP(w, r)

		require.Equal(t, http.StatusUnauthorized, w.Code, "authentication should not succeed")
		body, err := io.ReadAll(w.Body)
		require.NoError(t, err, "reading response body should succeed")
		require.Equal(t, "Failed to read writeKey from Query Params\n", string(body))
	})
}

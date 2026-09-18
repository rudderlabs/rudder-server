package backendconfig

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
)

func TestFetchStatsDoer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fail" {
			w.WriteHeader(http.StatusBadGateway)
		}
		_, _ = w.Write([]byte(`{"payload":"0123456789"}`))
	}))
	t.Cleanup(srv.Close)
	store, err := memstats.New()
	require.NoError(t, err)
	d := &fetchStatsDoer{doer: srv.Client(), stats: store, version: "v2"}
	get := func(url string) {
		req, err := http.NewRequest(http.MethodGet, url, http.NoBody)
		require.NoError(t, err)
		resp, err := d.Do(req)
		require.NoError(t, err)
		defer func() { require.NoError(t, resp.Body.Close()) }()
		_, err = io.ReadAll(resp.Body)
		require.NoError(t, err)
	}

	get(srv.URL + "/config")
	get(srv.URL + "/config?updatedAfter=2026-09-15T00:00:00.000Z")
	get(srv.URL + "/config?updatedAfter=2026-09-15T00:00:00.000Z")
	get(srv.URL + "/fail")

	full := stats.Tags{"version": "v2", "mode": "full"}
	incremental := stats.Tags{"version": "v2", "mode": "incremental"}
	require.Equal(t, 1.0, counterValue(store, "backend_config_http_calls", stats.Tags{"version": "v2", "mode": "full", "error": ""}))
	require.Equal(t, 1.0, counterValue(store, "backend_config_http_calls", stats.Tags{"version": "v2", "mode": "full", "error": "http_502"}))
	require.Equal(t, 2.0, counterValue(store, "backend_config_http_calls", stats.Tags{"version": "v2", "mode": "incremental", "error": ""}))
	require.Equal(t, []float64{24, 24}, store.Get("backend_config_http_response_size", full).Values())
	require.Equal(t, []float64{24, 24}, store.Get("backend_config_http_response_size", incremental).Values())
}

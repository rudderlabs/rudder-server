package gateway

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
)

func TestAuth(t *testing.T) {
	delegate := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})

	newGateway := func(writeKeysSourceMap, sourceIDSourceMap map[string]backendconfig.SourceT) *Handle {
		return &Handle{
			logger:             logger.NOP,
			stats:              stats.Default,
			writeKeysSourceMap: writeKeysSourceMap,
			sourceIDSourceMap:  sourceIDSourceMap,
		}
	}

	newWriteKeyRequest := func(writeKey string) *http.Request {
		r := httptest.NewRequest("GET", "/", nil)
		if writeKey != "" {
			r.SetBasicAuth(writeKey, "")
		}
		r = r.WithContext(context.WithValue(r.Context(), gwtypes.CtxParamCallType, "dummy"))
		return r
	}

	newSourceIDRequest := func(sourceID string) *http.Request {
		r := httptest.NewRequest("GET", "/", nil)
		if sourceID != "" {
			r.Header.Add("X-Rudder-Source-Id", sourceID)
		}
		r = r.WithContext(context.WithValue(r.Context(), gwtypes.CtxParamCallType, "dummy"))
		return r
	}

	t.Run("writeKeyAuth", func(t *testing.T) {
		t.Run("successful auth", func(t *testing.T) {
			writeKey := "123"
			gw := newGateway(map[string]backendconfig.SourceT{
				writeKey: {
					Enabled: true,
				},
			}, nil)
			r := newWriteKeyRequest(writeKey)
			w := httptest.NewRecorder()
			gw.writeKeyAuth(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusOK, w.Code, "authentication should succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "OK", string(body))
		})

		t.Run("no writeKey", func(t *testing.T) {
			gw := newGateway(nil, nil)
			r := newWriteKeyRequest("")
			w := httptest.NewRecorder()
			gw.writeKeyAuth(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusUnauthorized, w.Code, "authentication should not succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "Failed to read writeKey from header\n", string(body))
		})

		t.Run("invalid writeKey", func(t *testing.T) {
			gw := newGateway(nil, nil)
			r := newWriteKeyRequest("random")
			w := httptest.NewRecorder()
			gw.writeKeyAuth(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusUnauthorized, w.Code, "authentication should not succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "Invalid Write Key\n", string(body))
		})

		t.Run("disabled source", func(t *testing.T) {
			writeKey := "123"
			gw := newGateway(map[string]backendconfig.SourceT{
				writeKey: {
					Enabled: false,
				},
			}, nil)
			r := newWriteKeyRequest(writeKey)
			w := httptest.NewRecorder()
			gw.writeKeyAuth(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusNotFound, w.Code, "authentication should not succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "Source is disabled\n", string(body))
		})
	})

	t.Run("webhookAuth", func(t *testing.T) {
		t.Run("successful auth with authorization header", func(t *testing.T) {
			writeKey := "123"
			gw := newGateway(map[string]backendconfig.SourceT{
				writeKey: {
					Enabled: true,
					SourceDefinition: backendconfig.SourceDefinitionT{
						Category: "webhook",
					},
				},
			}, nil)
			r := newWriteKeyRequest(writeKey)
			w := httptest.NewRecorder()
			gw.webhookAuth(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusOK, w.Code, "authentication should succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "OK", string(body))
		})

		t.Run("successful auth with query param", func(t *testing.T) {
			writeKey := "123"
			gw := newGateway(map[string]backendconfig.SourceT{
				writeKey: {
					Enabled: true,
					SourceDefinition: backendconfig.SourceDefinitionT{
						Category: "webhook",
					},
				},
			}, nil)
			r := newWriteKeyRequest("")

			params := url.Values{}
			params.Add("writeKey", writeKey)
			r.URL.RawQuery = params.Encode()
			w := httptest.NewRecorder()
			gw.webhookAuth(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusOK, w.Code, "authentication should succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "OK", string(body))
		})

		t.Run("no writeKey", func(t *testing.T) {
			gw := newGateway(nil, nil)
			r := newWriteKeyRequest("")
			w := httptest.NewRecorder()
			gw.webhookAuth(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusUnauthorized, w.Code, "authentication should not succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "Failed to read writeKey from Query Params\n", string(body))
		})

		t.Run("invalid writeKey", func(t *testing.T) {
			gw := newGateway(nil, nil)
			r := newWriteKeyRequest("random")
			w := httptest.NewRecorder()
			gw.webhookAuth(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusUnauthorized, w.Code, "authentication should not succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "Invalid Write Key\n", string(body))
		})

		t.Run("not a webhook source", func(t *testing.T) {
			writeKey := "123"
			gw := newGateway(map[string]backendconfig.SourceT{
				writeKey: {
					Enabled: true,
					SourceDefinition: backendconfig.SourceDefinitionT{
						Category: "other",
					},
				},
			}, nil)
			r := newWriteKeyRequest(writeKey)
			w := httptest.NewRecorder()
			gw.webhookAuth(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusUnauthorized, w.Code, "authentication should not succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "Invalid Write Key\n", string(body))
		})

		t.Run("disabled webhook source", func(t *testing.T) {
			writeKey := "123"
			gw := newGateway(map[string]backendconfig.SourceT{
				writeKey: {
					Enabled: false,
					SourceDefinition: backendconfig.SourceDefinitionT{
						Category: "webhook",
					},
				},
			}, nil)
			r := newWriteKeyRequest(writeKey)
			w := httptest.NewRecorder()
			gw.webhookAuth(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusNotFound, w.Code, "authentication should not succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "Source is disabled\n", string(body))
		})
	})

	t.Run("sourceIDAuth", func(t *testing.T) {
		t.Run("successful auth", func(t *testing.T) {
			sourceID := "123"
			gw := newGateway(nil, map[string]backendconfig.SourceT{
				sourceID: {
					Enabled: true,
				},
			})
			r := newSourceIDRequest(sourceID)
			w := httptest.NewRecorder()
			gw.sourceIDAuth(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusOK, w.Code, "authentication should succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "OK", string(body))
		})

		t.Run("no sourceID", func(t *testing.T) {
			gw := newGateway(nil, nil)
			r := newSourceIDRequest("")
			w := httptest.NewRecorder()
			gw.sourceIDAuth(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusUnauthorized, w.Code, "authentication should not succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "Failed to read source id from header\n", string(body))
		})

		t.Run("invalid writeKey", func(t *testing.T) {
			gw := newGateway(nil, nil)
			r := newSourceIDRequest("random")
			w := httptest.NewRecorder()
			gw.sourceIDAuth(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusUnauthorized, w.Code, "authentication should not succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "Invalid source id\n", string(body))
		})

		t.Run("disabled source", func(t *testing.T) {
			sourceID := "123"
			gw := newGateway(nil, map[string]backendconfig.SourceT{
				sourceID: {
					Enabled: false,
				},
			})
			r := newSourceIDRequest(sourceID)
			w := httptest.NewRecorder()
			gw.sourceIDAuth(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusNotFound, w.Code, "authentication should not succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "Source is disabled\n", string(body))
		})
	})
}

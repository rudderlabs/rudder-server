package gateway

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
)

func TestAuth(t *testing.T) {
	delegate := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})
	statsStore, err := memstats.New()
	require.NoError(t, err)

	newGateway := func(writeKeysSourceMap, sourceIDSourceMap map[string]backendconfig.SourceT) *Handle {
		return &Handle{
			logger:             logger.NOP,
			stats:              statsStore,
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

	newRequestWithSourceIDAndDestID := func(sourceID, destinationID, reqType string, reqCtx *gwtypes.AuthRequestContext) *http.Request {
		r := httptest.NewRequest("GET", "/", nil)
		if len(sourceID) != 0 {
			r.Header.Add("X-Rudder-Source-Id", sourceID)
		}
		if len(destinationID) != 0 {
			r.Header.Add("X-Rudder-Destination-Id", destinationID)
		}
		if len(reqType) > 0 {
			r = r.WithContext(context.WithValue(r.Context(), gwtypes.CtxParamCallType, reqType))
		}
		if reqCtx != nil {
			r = r.WithContext(context.WithValue(r.Context(), gwtypes.CtxParamAuthRequestContext, reqCtx))
		}
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
			require.Equal(t,
				float64(1),
				statsStore.Get(
					"gateway.write_key_requests",
					map[string]string{
						"source":      "noWriteKey",
						"sourceID":    "noWriteKey",
						"workspaceId": "",
						"writeKey":    "noWriteKey",
						"reqType":     "dummy",
						"sourceType":  "",
						"sdkVersion":  "",
					},
				).LastValue(),
			)
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
					Enabled:     false,
					ID:          "456",
					Name:        "789",
					WorkspaceID: "wrskpc",
					SourceDefinition: backendconfig.SourceDefinitionT{
						Category: "catA",
					},
					WriteKey: writeKey,
				},
			}, nil)
			r := newWriteKeyRequest(writeKey)
			w := httptest.NewRecorder()
			gw.writeKeyAuth(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusNotFound, w.Code, "authentication should not succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "Source is disabled\n", string(body))
			require.Equal(t,
				float64(1),
				statsStore.Get(
					"gateway.write_key_requests",
					map[string]string{
						"reqType":     "dummy",
						"sdkVersion":  "",
						"source":      "789_123",
						"sourceID":    "456",
						"sourceType":  "catA",
						"workspaceId": "wrskpc",
						"writeKey":    writeKey,
					},
				).LastValue(),
			)
			require.Equal(t,
				float64(1),
				statsStore.Get(
					"gateway.write_key_failed_requests",
					map[string]string{
						"reqType":     "dummy",
						"sdkVersion":  "",
						"source":      "789_123",
						"sourceID":    "456",
						"sourceType":  "catA",
						"workspaceId": "wrskpc",
						"writeKey":    writeKey,
						"reason":      "Source is disabled",
					},
				).LastValue(),
			)
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

	t.Run("replaySourceIDAuth", func(t *testing.T) {
		t.Run("replay source", func(t *testing.T) {
			sourceID := "123"
			gw := newGateway(nil, map[string]backendconfig.SourceT{
				sourceID: {
					ID:         sourceID,
					Enabled:    true,
					OriginalID: sourceID,
				},
			})
			r := newSourceIDRequest(sourceID)
			w := httptest.NewRecorder()
			gw.replaySourceIDAuth(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusOK, w.Code, "authentication should succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "OK", string(body))
		})

		t.Run("invalid source using replay endpoint", func(t *testing.T) {
			sourceID := "123"
			invalidSource := "345"
			gw := newGateway(nil, map[string]backendconfig.SourceT{
				sourceID: {
					ID:         sourceID,
					Enabled:    true,
					OriginalID: "",
				},
			})
			r := newSourceIDRequest(invalidSource)
			w := httptest.NewRecorder()
			gw.replaySourceIDAuth(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusUnauthorized, w.Code, "authentication should not succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "Invalid source id\n", string(body))
		})

		t.Run("regular source using replay endpoint", func(t *testing.T) {
			sourceID := "123"
			gw := newGateway(nil, map[string]backendconfig.SourceT{
				sourceID: {
					ID:         sourceID,
					Enabled:    true,
					OriginalID: "",
				},
			})
			r := newSourceIDRequest(sourceID)
			w := httptest.NewRecorder()
			gw.replaySourceIDAuth(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusUnauthorized, w.Code, "authentication should not succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "Invalid replay source\n", string(body))
		})
	})

	t.Run("authDestIDForSource", func(t *testing.T) {
		t.Run("successful auth with destination header", func(t *testing.T) {
			sourceID := "123"
			destinationID := "456"
			gw := newGateway(nil, map[string]backendconfig.SourceT{
				sourceID: {
					Enabled: true,
					Destinations: []backendconfig.DestinationT{{
						ID: destinationID,
					}},
				},
			})
			r := newRequestWithSourceIDAndDestID(sourceID, destinationID, "dummy", &gwtypes.AuthRequestContext{
				Source: backendconfig.SourceT{
					Destinations: []backendconfig.DestinationT{{ID: destinationID, Enabled: true}},
				},
			})
			w := httptest.NewRecorder()
			gw.authDestIDForSource(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusOK, w.Code, "authentication should succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "OK", string(body))
		})

		t.Run("auth req should be present in context", func(t *testing.T) {
			sourceID := "123"
			destinationID := "456"
			gw := newGateway(nil, map[string]backendconfig.SourceT{
				sourceID: {
					Enabled: true,
					Destinations: []backendconfig.DestinationT{{
						ID: destinationID,
					}},
				},
			})
			r := newRequestWithSourceIDAndDestID(sourceID, destinationID, "dummy", nil)
			w := httptest.NewRecorder()
			gw.authDestIDForSource(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusInternalServerError, w.Code, "authentication should succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "unable to get AuthRequest from context\n", string(body))
		})

		t.Run("req type should be present in context", func(t *testing.T) {
			sourceID := "123"
			destinationID := "456"
			gw := newGateway(nil, map[string]backendconfig.SourceT{
				sourceID: {
					Enabled: true,
					Destinations: []backendconfig.DestinationT{{
						ID: destinationID,
					}},
				},
			})
			r := newRequestWithSourceIDAndDestID(sourceID, destinationID, "", &gwtypes.AuthRequestContext{
				Source: backendconfig.SourceT{
					Destinations: []backendconfig.DestinationT{{ID: destinationID, Enabled: true}},
				},
			})
			w := httptest.NewRecorder()
			gw.authDestIDForSource(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusInternalServerError, w.Code, "authentication should succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "unable to get request type from context\n", string(body))
		})

		t.Run("successful auth without destination id in header", func(t *testing.T) {
			sourceID := "123"
			gw := newGateway(nil, map[string]backendconfig.SourceT{
				sourceID: {
					Enabled: true,
				},
			})
			gw.config = config.Default
			r := newRequestWithSourceIDAndDestID(sourceID, "", "dummy", &gwtypes.AuthRequestContext{})
			w := httptest.NewRecorder()
			gw.authDestIDForSource(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusOK, w.Code, "authentication should succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "OK", string(body))
		})

		t.Run("failed auth without destination id in header", func(t *testing.T) {
			sourceID := "123"
			gw := newGateway(nil, map[string]backendconfig.SourceT{
				sourceID: {
					Enabled: true,
				},
			})
			gw.config = config.Default
			gw.config.Set("Gateway.requireDestinationIdHeader", true)
			r := newRequestWithSourceIDAndDestID(sourceID, "", "dummy", &gwtypes.AuthRequestContext{})
			w := httptest.NewRecorder()
			gw.authDestIDForSource(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusBadRequest, w.Code, "authentication should succeed")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "Failed to read destination id from header\n", string(body))
		})

		t.Run("invalid destination id", func(t *testing.T) {
			sourceID := "123"
			destinationID := "456"
			gw := newGateway(nil, map[string]backendconfig.SourceT{
				sourceID: {
					Enabled: true,
					Destinations: []backendconfig.DestinationT{{
						ID: destinationID,
					}},
				},
			})
			r := newRequestWithSourceIDAndDestID(sourceID, destinationID, "dummy", &gwtypes.AuthRequestContext{
				Source: backendconfig.SourceT{
					Destinations: []backendconfig.DestinationT{{ID: "invalid-dest-id"}},
				},
			})
			w := httptest.NewRecorder()
			gw.authDestIDForSource(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusBadRequest, w.Code, "authentication should fail")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "Invalid destination id\n", string(body))
		})

		t.Run("destination disabled", func(t *testing.T) {
			sourceID := "123"
			destinationID := "456"
			gw := newGateway(nil, map[string]backendconfig.SourceT{
				sourceID: {
					Enabled: true,
					Destinations: []backendconfig.DestinationT{{
						ID: destinationID,
					}},
				},
			})
			r := newRequestWithSourceIDAndDestID(sourceID, destinationID, "dummy", &gwtypes.AuthRequestContext{
				Source: backendconfig.SourceT{
					Destinations: []backendconfig.DestinationT{{ID: destinationID, Enabled: false}},
				},
			})
			w := httptest.NewRecorder()
			gw.authDestIDForSource(delegate).ServeHTTP(w, r)

			require.Equal(t, http.StatusNotFound, w.Code, "authentication should fail")
			body, err := io.ReadAll(w.Body)
			require.NoError(t, err, "reading response body should succeed")
			require.Equal(t, "Destination is disabled\n", string(body))
		})
	})
}

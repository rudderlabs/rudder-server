package transformer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

func newTestFeaturesService(features *featuresPayload) *featuresService {
	handler := &featuresService{
		logger:   logger.NewLogger(),
		waitChan: make(chan struct{}),
		options: FeaturesServiceOptions{
			PollInterval: 10 * time.Millisecond,
		},
		client: &http.Client{},
	}
	handler.features.Store(features)
	return handler
}

func parseTestFeatures(t *testing.T, rawFeatures string) *featuresPayload {
	t.Helper()
	features, err := parseFeatures([]byte(rawFeatures))
	require.NoError(t, err)
	return features
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestTransformerFeatures(t *testing.T) {
	t.Run("Transformer features service", func(t *testing.T) {
		t.Run("SecretPaths", func(t *testing.T) {
			t.Run("normalizes the transformer response", func(t *testing.T) {
				testCases := []struct {
					name        string
					rawFeatures string
					destType    string
					wantPaths   []string
					wantOK      bool
				}{
					{name: "listed paths", rawFeatures: `{"secretPaths":{"KLAVIYO":["headers.Authorization","params.api_key"]}}`, destType: "KLAVIYO", wantPaths: []string{"headers.Authorization", "params.api_key"}, wantOK: true},
					{name: "empty list", rawFeatures: `{"secretPaths":{"EMPTY":[]}}`, destType: "EMPTY", wantPaths: []string{}, wantOK: true},
					{name: "null entry", rawFeatures: `{"secretPaths":{"UNRESOLVED":null}}`, destType: "UNRESOLVED", wantPaths: nil, wantOK: false},
					{name: "missing destination", rawFeatures: `{"secretPaths":{"KLAVIYO":[]}}`, destType: "MISSING", wantPaths: nil, wantOK: false},
					{name: "malformed entry", rawFeatures: `{"secretPaths":{"KLAVIYO":"bad","BRAZE":["headers.Authorization"]}}`, destType: "KLAVIYO", wantPaths: nil, wantOK: false},
					{name: "valid sibling of a malformed entry", rawFeatures: `{"secretPaths":{"KLAVIYO":"bad","BRAZE":["headers.Authorization"]}}`, destType: "BRAZE", wantPaths: []string{"headers.Authorization"}, wantOK: true},
					{name: "malformed table", rawFeatures: `{"secretPaths":"oops"}`, destType: "BRAZE", wantPaths: nil, wantOK: false},
					{name: "older transformer", rawFeatures: `{}`, destType: "KLAVIYO", wantPaths: nil, wantOK: true},
					{name: "top-level null", rawFeatures: `{"secretPaths":null}`, destType: "KLAVIYO", wantPaths: nil, wantOK: true},
				}
				for _, tc := range testCases {
					t.Run(tc.name, func(t *testing.T) {
						paths, ok := newTestFeaturesService(parseTestFeatures(t, tc.rawFeatures)).SecretPaths(tc.destType)
						require.Equal(t, tc.wantOK, ok)
						require.Equal(t, tc.wantPaths, paths)
					})
				}
			})

			t.Run("does not reject sibling features when the table is malformed", func(t *testing.T) {
				handler := newTestFeaturesService(parseTestFeatures(t, `{"routerTransform":{"BRAZE":true},"secretPaths":"oops"}`))
				require.True(t, handler.RouterTransform("BRAZE"))
			})

			t.Run("returns an empty successful lookup for the default and no-op services", func(t *testing.T) {
				paths, ok := newTestFeaturesService(defaultTransformerFeatures).SecretPaths("KLAVIYO")
				require.True(t, ok)
				require.Nil(t, paths)
				paths, ok = NewNoOpService().SecretPaths("KLAVIYO")
				require.True(t, ok)
				require.Nil(t, paths)
			})
		})

		t.Run("defaultTransformerFeatures must advertise a non-deprecated source transformer version", func(t *testing.T) {
			require.Equal(t, V2, defaultTransformerFeatures.sourceTransformerVersion())
		})

		t.Run("handler should wait till features are not fetched", func(t *testing.T) {
			handler := newTestFeaturesService(defaultTransformerFeatures)

			require.Never(t, func() bool {
				return isClosed(handler.Wait())
			}, 2*time.Second, 10*time.Millisecond)
		})

		t.Run("before features are fetched, SourceTransformerVersion should return v2(default)", func(t *testing.T) {
			handler := newTestFeaturesService(defaultTransformerFeatures)

			require.Equal(t, V2, handler.SourceTransformerVersion())
		})

		t.Run("before features are fetched, TransformerProxyVersion should return v0", func(t *testing.T) {
			handler := newTestFeaturesService(defaultTransformerFeatures)

			require.Equal(t, V0, handler.TransformerProxyVersion())
		})

		t.Run("before features are fetched, defaultTransformerFeatures must be served", func(t *testing.T) {
			handler := newTestFeaturesService(defaultTransformerFeatures)

			require.True(t, handler.RouterTransform("MARKETO"))
			require.True(t, handler.RouterTransform("HS"))
			require.False(t, handler.RouterTransform("ACTIVE_CAMPAIGN"))
			require.False(t, handler.RouterTransform("ALGOLIA"))
			require.Equal(t, []string{"AM"}, handler.Regulations())
			require.Equal(t, V2, handler.SourceTransformerVersion())
		})

		t.Run("if transformer returns an error status, features should not be considered fetched", func(t *testing.T) {
			transformerServer := httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Error(w, "error", http.StatusInternalServerError)
				}))
			t.Cleanup(transformerServer.Close)

			handler := newTestFeaturesService(defaultTransformerFeatures)
			handler.options.TransformerURL = transformerServer.URL

			require.ErrorContains(t, handler.makeFeaturesFetchCall(t.Context()), "unexpected response status")

			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan struct{})
			go func() {
				defer close(done)
				handler.syncTransformerFeatureJson(ctx)
			}()
			t.Cleanup(func() {
				cancel()
				<-done
			})

			require.Never(t, func() bool {
				return isClosed(handler.Wait())
			}, 500*time.Millisecond, 10*time.Millisecond)
			require.True(t, handler.RouterTransform("MARKETO")) // still serving defaults
		})

		t.Run("if transformer has no /features endpoint, the defaults should be served and the fetch considered successful", func(t *testing.T) {
			transformerServer := httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Error(w, "not found", http.StatusNotFound)
				}))
			t.Cleanup(transformerServer.Close)

			handler := newTestFeaturesService(parseTestFeatures(t, `{"routerTransform": {"CUSTOMERIO": true}}`))
			handler.options.TransformerURL = transformerServer.URL

			require.NoError(t, handler.makeFeaturesFetchCall(t.Context()))
			require.Same(t, defaultTransformerFeatures, handler.features.Load())

			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan struct{})
			go func() {
				defer close(done)
				handler.syncTransformerFeatureJson(ctx)
			}()
			t.Cleanup(func() {
				cancel()
				<-done
			})

			require.Eventually(t, func() bool {
				return isClosed(handler.Wait())
			}, time.Second, 10*time.Millisecond)
			require.True(t, handler.RouterTransform("MARKETO"))
			require.False(t, handler.RouterTransform("CUSTOMERIO"))
		})

		t.Run("should release Wait() when the context is cancelled before the first successful fetch", func(t *testing.T) {
			transformerServer := httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Error(w, "error", http.StatusInternalServerError)
				}))
			t.Cleanup(transformerServer.Close)

			handler := newTestFeaturesService(defaultTransformerFeatures)
			handler.options.TransformerURL = transformerServer.URL

			ctx, cancel := context.WithCancel(t.Context())
			go handler.syncTransformerFeatureJson(ctx)

			require.Never(t, func() bool {
				return isClosed(handler.Wait())
			}, 100*time.Millisecond, 10*time.Millisecond)
			cancel()
			require.Eventually(t, func() bool {
				return isClosed(handler.Wait())
			}, time.Second, 10*time.Millisecond)
		})

		t.Run("should not swap the features snapshot when the fetched body is unchanged", func(t *testing.T) {
			mockTransformerResp := `{"supportSourceTransformV1": true}`
			transformerServer := httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(mockTransformerResp))
				}))
			t.Cleanup(transformerServer.Close)

			handler := newTestFeaturesService(defaultTransformerFeatures)
			handler.options.TransformerURL = transformerServer.URL

			require.NoError(t, handler.makeFeaturesFetchCall(t.Context()))
			firstSnapshot := handler.features.Load()
			require.NoError(t, handler.makeFeaturesFetchCall(t.Context()))
			require.Same(t, firstSnapshot, handler.features.Load())
		})

		t.Run("If source transform is not v1 or v2, it should panic on the first poll, before Wait() releases", func(t *testing.T) {
			handler := newTestFeaturesService(defaultTransformerFeatures)

			mockTransformerResp := `{
				"routerTransform": {
				  "a": true,
				  "b": true
				},
				"regulations": ["AM"],
				"supportSourceTransformV1": false,
				"supportTransformerProxyV1": true
			  }`
			transformerServer := httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(mockTransformerResp))
				}))
			t.Cleanup(transformerServer.Close)

			handler.options.TransformerURL = transformerServer.URL
			require.PanicsWithValue(t,
				"Webhook source v0 version has been deprecated. This is a breaking change. Upgrade transformer version to greater than 1.50.0 for v1",
				func() { handler.syncTransformerFeatureJson(t.Context()) },
			)
			require.False(t, isClosed(handler.Wait()))
		})

		t.Run("Get should return features fetched from transformer", func(t *testing.T) {
			mockTransformerResp := `{
				"routerTransform": {
				  "a": true,
				  "b": true
				},
				"regulations": ["AM"],
				"supportSourceTransformV1": true,
				"upgradedToSourceTransformV2": true,
				"supportTransformerProxyV1": true
			  }`
			transformerServer := httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(mockTransformerResp))
				}))
			t.Cleanup(transformerServer.Close)

			handler := NewFeaturesService(t.Context(), config.Default, FeaturesServiceOptions{
				PollInterval:   10 * time.Millisecond,
				TransformerURL: transformerServer.URL,
			})

			<-handler.Wait()

			require.False(t, handler.RouterTransform("MARKETO"))
			require.False(t, handler.RouterTransform("HS"))
			require.True(t, handler.RouterTransform("a"))
			require.True(t, handler.RouterTransform("b"))
			require.Equal(t, V2, handler.SourceTransformerVersion())
			require.Equal(t, V1, handler.TransformerProxyVersion())
			require.Equal(t, []string{"AM"}, handler.Regulations())
		})

		t.Run("Get should return empty array when features doesn't have regulations", func(t *testing.T) {
			featuresService := newTestFeaturesService(parseTestFeatures(t, `{}`))

			require.Equal(t, []string{}, featuresService.Regulations())
		})

		t.Run("Get should return empty array when features has empty regulations", func(t *testing.T) {
			featuresService := newTestFeaturesService(parseTestFeatures(t, `{
				"regulations": []
			}`))

			require.Equal(t, []string{}, featuresService.Regulations())
		})

		t.Run("Get should return regulations when feature has regultions", func(t *testing.T) {
			featuresService := newTestFeaturesService(parseTestFeatures(t, `{
				"regulations": ["AM"]
			}`))

			require.Equal(t, []string{"AM"}, featuresService.Regulations())
		})

		t.Run("TransformerProxy should be true for a destination the transformer declares", func(t *testing.T) {
			featuresService := newTestFeaturesService(parseTestFeatures(t, `{
				"transformerProxy": {"CUSTOMERIO": true}
			}`))

			require.True(t, featuresService.TransformerProxy("CUSTOMERIO"))
			require.False(t, featuresService.TransformerProxy("MONDAY"))
		})

		t.Run("TransformerProxy should be false when the transformer image predates the capability", func(t *testing.T) {
			featuresService := newTestFeaturesService(parseTestFeatures(t, `{
				"routerTransform": {"CUSTOMERIO": true}
			}`))

			require.False(t, featuresService.TransformerProxy("CUSTOMERIO"))
		})

		t.Run("TransformerProxy should not confuse the capability map with the proxy protocol version", func(t *testing.T) {
			featuresService := newTestFeaturesService(parseTestFeatures(t, `{
				"supportTransformerProxyV1": true,
				"transformerProxy": {}
			}`))

			require.False(t, featuresService.TransformerProxy("CUSTOMERIO"))
			require.Equal(t, V1, featuresService.TransformerProxyVersion())
		})
	})
}

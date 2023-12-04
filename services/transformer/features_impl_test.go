package transformer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

var _ = Describe("Transformer features", func() {
	Context("Transformer features service", func() {
		It("handler should wait till features are not fetched", func() {
			handler := &featuresService{
				logger:   logger.NewLogger(),
				waitChan: make(chan struct{}),
				config: FeaturesServiceConfig{
					PollInterval:             time.Duration(1),
					FeaturesRetryMaxAttempts: 1,
				},
			}

			Consistently(func() bool {
				select {
				case <-handler.Wait():
					return true
				default:
					return false
				}
			}, 2*time.Second, 10*time.Millisecond).Should(BeFalse())
		})

		It("before features are fetched, SourceTransformerVersion should return v0", func() {
			handler := &featuresService{
				features: json.RawMessage(defaultTransformerFeatures),
				logger:   logger.NewLogger(),
				waitChan: make(chan struct{}),
				config: FeaturesServiceConfig{
					PollInterval:             time.Duration(1),
					FeaturesRetryMaxAttempts: 1,
				},
			}

			Expect(handler.SourceTransformerVersion()).To(Equal(V0))
		})

		It("before features are fetched, TransformerProxyVersion should return v0", func() {
			handler := &featuresService{
				features: json.RawMessage(defaultTransformerFeatures),
				logger:   logger.NewLogger(),
				waitChan: make(chan struct{}),
				config: FeaturesServiceConfig{
					PollInterval:             time.Duration(1),
					FeaturesRetryMaxAttempts: 1,
				},
			}

			Expect(handler.TransformerProxyVersion()).To(Equal(V0))
		})

		It("before features are fetched, defaultTransformerFeatures must be served", func() {
			handler := &featuresService{
				features: json.RawMessage(defaultTransformerFeatures),
				logger:   logger.NewLogger(),
				waitChan: make(chan struct{}),
				config: FeaturesServiceConfig{
					PollInterval:             time.Duration(1),
					FeaturesRetryMaxAttempts: 1,
				},
			}

			Expect(handler.RouterTransform("MARKETO")).To(BeTrue())
			Expect(handler.RouterTransform("HS")).To(BeTrue())
			Expect(handler.RouterTransform("ACTIVE_CAMPAIGN")).To(BeFalse())
			Expect(handler.RouterTransform("ALGOLIA")).To(BeFalse())
		})

		It("if transformer returns 404, features should be same as defaultTransformerFeatures", func() {
			transformerServer := httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Error(w, "not found error", http.StatusNotFound)
				}))

			handler := NewFeaturesService(context.TODO(), FeaturesServiceConfig{
				PollInterval:             time.Duration(1),
				TransformerURL:           transformerServer.URL,
				FeaturesRetryMaxAttempts: 1,
			})

			<-handler.Wait()

			Expect(handler.RouterTransform("MARKETO")).To(BeTrue())
			Expect(handler.RouterTransform("HS")).To(BeTrue())
			Expect(handler.RouterTransform("ACTIVE_CAMPAIGN")).To(BeFalse())
			Expect(handler.RouterTransform("ALGOLIA")).To(BeFalse())
		})

		It("Get should return features fetched from transformer", func() {
			mockTransformerResp := `{
				"routerTransform": {
				  "a": true,
				  "b": true
				},
				"supportSourceTransformV1": true,
				"supportTransformerProxyV1": true
			  }`
			transformerServer := httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(mockTransformerResp))
				}))

			handler := NewFeaturesService(context.TODO(), FeaturesServiceConfig{
				PollInterval:             time.Duration(1),
				TransformerURL:           transformerServer.URL,
				FeaturesRetryMaxAttempts: 1,
			})

			<-handler.Wait()

			Expect(handler.RouterTransform("MARKETO")).To(BeFalse())
			Expect(handler.RouterTransform("HS")).To(BeFalse())
			Expect(handler.RouterTransform("a")).To(BeTrue())
			Expect(handler.RouterTransform("b")).To(BeTrue())
			Expect(handler.SourceTransformerVersion()).To(Equal(V1))
			Expect(handler.TransformerProxyVersion()).To(Equal(V1))
		})
	})
})

package transformer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

var _ = Describe("Transformer features", func() {
	Context("Transformer features service", func() {
		It("handler should wait till features are not fetched", func() {
			handler := &featuresService{
				logger:   logger.NewLogger(),
				waitChan: make(chan struct{}),
				options: FeaturesServiceOptions{
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

		It("before features are fetched, SourceTransformerVersion should return v1(default) because v0 is deprecated", func() {
			handler := &featuresService{
				features: json.RawMessage(defaultTransformerFeatures),
				logger:   logger.NewLogger(),
				waitChan: make(chan struct{}),
				options: FeaturesServiceOptions{
					PollInterval:             time.Duration(1),
					FeaturesRetryMaxAttempts: 1,
				},
			}

			Expect(handler.SourceTransformerVersion()).To(Equal(V1))
		})

		It("before features are fetched, TransformerProxyVersion should return v0", func() {
			handler := &featuresService{
				features: json.RawMessage(defaultTransformerFeatures),
				logger:   logger.NewLogger(),
				waitChan: make(chan struct{}),
				options: FeaturesServiceOptions{
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
				options: FeaturesServiceOptions{
					PollInterval:             time.Duration(1),
					FeaturesRetryMaxAttempts: 1,
				},
			}

			Expect(handler.RouterTransform("MARKETO")).To(BeTrue())
			Expect(handler.RouterTransform("HS")).To(BeTrue())
			Expect(handler.RouterTransform("ACTIVE_CAMPAIGN")).To(BeFalse())
			Expect(handler.RouterTransform("ALGOLIA")).To(BeFalse())
			Expect(handler.Regulations()).To(Equal([]string{"AM"}))
			Expect(handler.SourceTransformerVersion()).To(Equal(V1))
		})

		It("if transformer returns 404, features should be same as defaultTransformerFeatures", func() {
			transformerServer := httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Error(w, "not found error", http.StatusNotFound)
				}))

			handler := NewFeaturesService(context.TODO(), config.Default, FeaturesServiceOptions{
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

		It("If source transform is not v1, it should panic as v0 is deprecated", func() {
			defer func() {
				if r := recover(); r == nil {
					Fail("The function `SourceTransformerVersion()` is supposed to panic. It did not.")
				} else {
					if err, ok := r.(error); ok {
						Expect(err.Error()).To(Equal("Webhook source v0 version has been deprecated. This is a breaking change. Upgrade transformer version to greater than 1.50.0 for v1"))
					} else {
						Expect(r).To(Equal("Webhook source v0 version has been deprecated. This is a breaking change. Upgrade transformer version to greater than 1.50.0 for v1"))
					}
				}
			}()

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

			featConfig := FeaturesServiceOptions{
				PollInterval:             time.Duration(1),
				TransformerURL:           transformerServer.URL,
				FeaturesRetryMaxAttempts: 1,
			}

			handler := &featuresService{
				features: json.RawMessage(defaultTransformerFeatures),
				logger:   logger.NewLogger().Child("transformer-features"),
				waitChan: make(chan struct{}),
				options:  featConfig,
				client: &http.Client{
					Transport: &http.Transport{
						DisableKeepAlives:   config.Default.GetBool("Transformer.Client.disableKeepAlives", true),
						MaxConnsPerHost:     config.Default.GetInt("Transformer.Client.maxHTTPConnections", 100),
						MaxIdleConnsPerHost: config.Default.GetInt("Transformer.Client.maxHTTPIdleConnections", 10),
						IdleConnTimeout:     config.Default.GetDuration("Transformer.Client.maxIdleConnDuration", 30, time.Second),
					},
					Timeout: config.Default.GetDuration("HttpClient.processor.timeout", 30, time.Second),
				},
			}
			handler.syncTransformerFeatureJson(context.TODO())

			<-handler.Wait()

			handler.SourceTransformerVersion()
		})

		It("Get should return features fetched from transformer", func() {
			mockTransformerResp := `{
				"routerTransform": {
				  "a": true,
				  "b": true
				},
				"regulations": ["AM"],
				"supportSourceTransformV1": true,
				"supportTransformerProxyV1": true
			  }`
			transformerServer := httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(mockTransformerResp))
				}))

			handler := NewFeaturesService(context.TODO(), config.Default, FeaturesServiceOptions{
				PollInterval:             time.Duration(1),
				TransformerURL:           transformerServer.URL,
				FeaturesRetryMaxAttempts: 1,
			})

			<-handler.Wait()

			Expect(handler.RouterTransform("MARKETO")).To(BeFalse())
			Expect(handler.RouterTransform("HS")).To(BeFalse())
			Expect(handler.RouterTransform("a")).To(BeTrue())
			Expect(handler.RouterTransform("b")).To(BeTrue())
			Expect(handler.SourceTransformerVersion()).To(Equal(V1)) // V1 is default (V0 is deprecated)
			Expect(handler.TransformerProxyVersion()).To(Equal(V1))
			Expect(handler.Regulations()).To(Equal([]string{"AM"}))
		})

		It("Get should return empty array when features doesn't have regulations", func() {
			featuresService := &featuresService{
				features: json.RawMessage(`{}`),
			}

			Expect(featuresService.Regulations()).To(Equal([]string{}))
		})

		It("Get should return empty array when features has empty regulations", func() {
			featuresService := &featuresService{
				features: json.RawMessage(`{
					"regulations": []
				}`),
			}

			Expect(featuresService.Regulations()).To(Equal([]string{}))
		})

		It("Get should return regulations when feature has regultions", func() {
			featuresService := &featuresService{
				features: json.RawMessage(`{
					"regulations": ["AM"]
				}`),
			}

			Expect(featuresService.Regulations()).To(Equal([]string{"AM"}))
		})
	})
})

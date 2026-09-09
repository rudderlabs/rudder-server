package transformer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

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

func parseTestFeatures(rawFeatures string) *featuresPayload {
	features, err := parseFeatures([]byte(rawFeatures))
	Expect(err).ToNot(HaveOccurred())
	return features
}

var _ = Describe("Transformer features", func() {
	Context("Transformer features service", func() {
		It("defaultTransformerFeatures must advertise a non-deprecated source transformer version", func() {
			Expect(defaultTransformerFeatures.sourceTransformerVersion()).To(Equal(V2))
		})

		It("handler should wait till features are not fetched", func() {
			handler := newTestFeaturesService(defaultTransformerFeatures)

			Consistently(func() bool {
				select {
				case <-handler.Wait():
					return true
				default:
					return false
				}
			}, 2*time.Second, 10*time.Millisecond).Should(BeFalse())
		})

		It("before features are fetched, SourceTransformerVersion should return v2(default)", func() {
			handler := newTestFeaturesService(defaultTransformerFeatures)

			Expect(handler.SourceTransformerVersion()).To(Equal(V2))
		})

		It("before features are fetched, TransformerProxyVersion should return v0", func() {
			handler := newTestFeaturesService(defaultTransformerFeatures)

			Expect(handler.TransformerProxyVersion()).To(Equal(V0))
		})

		It("before features are fetched, defaultTransformerFeatures must be served", func() {
			handler := newTestFeaturesService(defaultTransformerFeatures)

			Expect(handler.RouterTransform("MARKETO")).To(BeTrue())
			Expect(handler.RouterTransform("HS")).To(BeTrue())
			Expect(handler.RouterTransform("ACTIVE_CAMPAIGN")).To(BeFalse())
			Expect(handler.RouterTransform("ALGOLIA")).To(BeFalse())
			Expect(handler.Regulations()).To(Equal([]string{"AM"}))
			Expect(handler.SourceTransformerVersion()).To(Equal(V2))
		})

		It("if transformer returns an error status, features should not be considered fetched", func() {
			transformerServer := httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Error(w, "error", http.StatusInternalServerError)
				}))
			DeferCleanup(transformerServer.Close)

			handler := newTestFeaturesService(defaultTransformerFeatures)
			handler.options.TransformerURL = transformerServer.URL

			Expect(handler.makeFeaturesFetchCall(context.Background())).To(MatchError(ContainSubstring("unexpected response status")))

			ctx, cancel := context.WithCancel(context.Background())
			DeferCleanup(cancel)
			go handler.syncTransformerFeatureJson(ctx)

			Consistently(func() bool {
				select {
				case <-handler.Wait():
					return true
				default:
					return false
				}
			}, 500*time.Millisecond, 10*time.Millisecond).Should(BeFalse())
			Expect(handler.RouterTransform("MARKETO")).To(BeTrue()) // still serving defaults
		})

		It("if transformer has no /features endpoint, the defaults should be served and the fetch considered successful", func() {
			transformerServer := httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Error(w, "not found", http.StatusNotFound)
				}))
			DeferCleanup(transformerServer.Close)

			handler := newTestFeaturesService(parseTestFeatures(`{"routerTransform": {"CUSTOMERIO": true}}`))
			handler.options.TransformerURL = transformerServer.URL

			Expect(handler.makeFeaturesFetchCall(context.Background())).To(Succeed())
			Expect(handler.features.Load()).To(BeIdenticalTo(defaultTransformerFeatures))

			ctx, cancel := context.WithCancel(context.Background())
			DeferCleanup(cancel)
			go handler.syncTransformerFeatureJson(ctx)

			Eventually(handler.Wait(), time.Second).Should(BeClosed())
			Expect(handler.RouterTransform("MARKETO")).To(BeTrue())
			Expect(handler.RouterTransform("CUSTOMERIO")).To(BeFalse())
		})

		It("should release Wait() when the context is cancelled before the first successful fetch", func() {
			transformerServer := httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Error(w, "error", http.StatusInternalServerError)
				}))
			DeferCleanup(transformerServer.Close)

			handler := newTestFeaturesService(defaultTransformerFeatures)
			handler.options.TransformerURL = transformerServer.URL

			ctx, cancel := context.WithCancel(context.Background())
			go handler.syncTransformerFeatureJson(ctx)

			Consistently(handler.Wait(), 100*time.Millisecond, 10*time.Millisecond).ShouldNot(BeClosed())
			cancel()
			Eventually(handler.Wait(), time.Second).Should(BeClosed())
		})

		It("should not swap the features snapshot when the fetched body is unchanged", func() {
			mockTransformerResp := `{"supportSourceTransformV1": true}`
			transformerServer := httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(mockTransformerResp))
				}))
			DeferCleanup(transformerServer.Close)

			handler := newTestFeaturesService(defaultTransformerFeatures)
			handler.options.TransformerURL = transformerServer.URL

			Expect(handler.makeFeaturesFetchCall(context.Background())).To(Succeed())
			firstSnapshot := handler.features.Load()
			Expect(handler.makeFeaturesFetchCall(context.Background())).To(Succeed())
			Expect(handler.features.Load()).To(BeIdenticalTo(firstSnapshot))
		})

		It("If source transform is not v1 or v2, it should panic on the first poll, before Wait() releases", func() {
			handler := newTestFeaturesService(defaultTransformerFeatures)

			defer func() {
				r := recover()
				Expect(r).To(Equal("Webhook source v0 version has been deprecated. This is a breaking change. Upgrade transformer version to greater than 1.50.0 for v1"))
				Expect(handler.Wait()).ToNot(BeClosed())
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
			DeferCleanup(transformerServer.Close)

			handler.options.TransformerURL = transformerServer.URL
			handler.syncTransformerFeatureJson(context.TODO())
		})

		It("Get should return features fetched from transformer", func() {
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
			DeferCleanup(transformerServer.Close)

			handler := NewFeaturesService(context.TODO(), config.Default, FeaturesServiceOptions{
				PollInterval:   10 * time.Millisecond,
				TransformerURL: transformerServer.URL,
			})

			<-handler.Wait()

			Expect(handler.RouterTransform("MARKETO")).To(BeFalse())
			Expect(handler.RouterTransform("HS")).To(BeFalse())
			Expect(handler.RouterTransform("a")).To(BeTrue())
			Expect(handler.RouterTransform("b")).To(BeTrue())
			Expect(handler.SourceTransformerVersion()).To(Equal(V2))
			Expect(handler.TransformerProxyVersion()).To(Equal(V1))
			Expect(handler.Regulations()).To(Equal([]string{"AM"}))
		})

		It("Get should return empty array when features doesn't have regulations", func() {
			featuresService := newTestFeaturesService(parseTestFeatures(`{}`))

			Expect(featuresService.Regulations()).To(Equal([]string{}))
		})

		It("Get should return empty array when features has empty regulations", func() {
			featuresService := newTestFeaturesService(parseTestFeatures(`{
				"regulations": []
			}`))

			Expect(featuresService.Regulations()).To(Equal([]string{}))
		})

		It("Get should return regulations when feature has regultions", func() {
			featuresService := newTestFeaturesService(parseTestFeatures(`{
				"regulations": ["AM"]
			}`))

			Expect(featuresService.Regulations()).To(Equal([]string{"AM"}))
		})

		It("TransformerProxy should be true for a destination the transformer declares", func() {
			featuresService := newTestFeaturesService(parseTestFeatures(`{
				"transformerProxy": {"CUSTOMERIO": true}
			}`))

			Expect(featuresService.TransformerProxy("CUSTOMERIO")).To(BeTrue())
			Expect(featuresService.TransformerProxy("MONDAY")).To(BeFalse())
		})

		It("TransformerProxy should be false when the transformer image predates the capability", func() {
			featuresService := newTestFeaturesService(parseTestFeatures(`{
				"routerTransform": {"CUSTOMERIO": true}
			}`))

			Expect(featuresService.TransformerProxy("CUSTOMERIO")).To(BeFalse())
		})

		It("TransformerProxy should not confuse the capability map with the proxy protocol version", func() {
			featuresService := newTestFeaturesService(parseTestFeatures(`{
				"supportTransformerProxyV1": true,
				"transformerProxy": {}
			}`))

			Expect(featuresService.TransformerProxy("CUSTOMERIO")).To(BeFalse())
			Expect(featuresService.TransformerProxyVersion()).To(Equal(V1))
		})
	})
})

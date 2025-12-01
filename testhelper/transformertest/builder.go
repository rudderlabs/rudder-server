package transformertest

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"

	routerTypes "github.com/rudderlabs/rudder-server/router/types"

	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-server/processor/types"
)

// NewBuilder returns a new test transformer Builder
func NewBuilder() *Builder {
	b := &Builder{
		routerTransforms:      map[string]struct{}{},
		destTransformHandlers: map[string]http.HandlerFunc{},
		srcHydrationHandlers:  map[string]http.HandlerFunc{},
	}
	return b
}

// Builder is a builder for a test transformer server
type Builder struct {
	routerTransforms            map[string]struct{}
	userTransformHandler        http.HandlerFunc
	destTransformHandlers       map[string]http.HandlerFunc
	trackingPlanHandler         http.HandlerFunc
	routerTransformHandler      http.HandlerFunc
	routerBatchTransformHandler http.HandlerFunc
	featuresHandler             http.HandlerFunc
	srcHydrationHandlers        map[string]http.HandlerFunc
}

func (b *Builder) WithFeaturesHandler(h http.HandlerFunc) *Builder {
	b.featuresHandler = h
	return b
}

// WithRouterTransformHandlerFunc sets the router transformation http handler function for the server
func (b *Builder) WithRouterTransformHandlerFunc(h http.HandlerFunc) *Builder {
	b.routerTransformHandler = apiVersionMiddleware(h)
	return b
}

// WithRouterTransformHandler sets the router transformation handler for the server
func (b *Builder) WithRouterTransformHandler(h RouterTransformerHandler) *Builder {
	return b.WithRouterTransformHandlerFunc(routerTransformerFunc(h))
}

// WithRouterBatchTransformHandlerFunc sets the router batch transformation http handler function for the server
func (b *Builder) WithRouterBatchTransformHandlerFunc(h http.HandlerFunc) *Builder {
	b.routerBatchTransformHandler = apiVersionMiddleware(h)
	return b
}

// WithRouterBatchTransformHandler sets the router batch transformation handler for the server
func (b *Builder) WithRouterBatchTransformHandler(h RouterTransformerHandler) *Builder {
	return b.WithRouterBatchTransformHandlerFunc(routerBatchTransformerFunc(h))
}

// WithUserTransformHandlerFunc sets the user transformation http handler function for the server
func (b *Builder) WithUserTransformHandlerFunc(h http.HandlerFunc) *Builder {
	b.userTransformHandler = apiVersionMiddleware(h)
	return b
}

// WithUserTransformHandler sets the user transformation handler for the server
func (b *Builder) WithUserTransformHandler(h TransformerHandler) *Builder {
	return b.WithUserTransformHandlerFunc(transformerFunc(h))
}

// WithDesTransformHandlerFunc sets a destination specific transformation http handler function for the server
func (b *Builder) WithDesTransformHandlerFunc(destType string, h http.HandlerFunc) *Builder {
	b.destTransformHandlers[destType] = apiVersionMiddleware(h)
	return b
}

func (b *Builder) WithSrcHydrationHandlerFunc(srcName string, h http.HandlerFunc) *Builder {
	b.srcHydrationHandlers[srcName] = h
	return b
}

func (b *Builder) WithSrcHydrationHandler(srcName string, h SrcHydrationHandler) *Builder {
	return b.WithSrcHydrationHandlerFunc(srcName, srcTransformerFunc(h))
}

// WithDestTransformHandler sets a destination specific transformation handler for the server
func (b *Builder) WithDestTransformHandler(destType string, h TransformerHandler) *Builder {
	return b.WithDesTransformHandlerFunc(destType, transformerFunc(h))
}

// WithTrackingPlanHandlerFunc sets the tracking plan validation http handler function for the server
func (b *Builder) WithTrackingPlanHandlerFunc(h http.HandlerFunc) *Builder {
	b.trackingPlanHandler = apiVersionMiddleware(h)
	return b
}

// WithTrackingPlanHandler sets the tracking plan validation handler for the server
func (b *Builder) WithTrackingPlanHandler(h TransformerHandler) *Builder {
	return b.WithTrackingPlanHandlerFunc(transformerFunc(h))
}

// WithRouterTransform enables router transformation for a specific destination type
func (b *Builder) WithRouterTransform(destType string) *Builder {
	b.routerTransforms[destType] = struct{}{}
	return b
}

// Build builds the test tranformer server
func (b *Builder) Build() *httptest.Server {
	// user/custom transformation
	if b.userTransformHandler == nil {
		b.userTransformHandler = apiVersionMiddleware(transformerFunc(MirroringTransformerHandler))
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/customTransform", b.userTransformHandler)

	// tracking plan validtion
	if b.trackingPlanHandler == nil {
		b.trackingPlanHandler = apiVersionMiddleware(transformerFunc(MirroringTransformerHandler))
	}
	mux.HandleFunc("/v0/validate", b.trackingPlanHandler)

	// destination transformation
	if _, ok := b.destTransformHandlers["*"]; !ok {
		b.destTransformHandlers["*"] = apiVersionMiddleware(transformerFunc(MirroringTransformerHandler))
	}
	for k := range b.destTransformHandlers {
		destHandler := b.destTransformHandlers[k]
		switch k {
		case "*":
			mux.HandleFunc("/v0/destinations/", destHandler)
		default:
			mux.HandleFunc("/v0/destinations/"+strings.ToLower(k), destHandler)
		}
	}

	for k := range b.srcHydrationHandlers {
		srcHydrateHandler := b.srcHydrationHandlers[k]
		switch k {
		case "*":
			mux.HandleFunc("/v2/sources/", srcHydrateHandler)
		default:
			mux.HandleFunc("/v2/sources/"+strings.ToLower(k)+"/hydrate", srcHydrateHandler)
		}
	}

	// router transformation
	if b.routerTransformHandler == nil {
		b.routerTransformHandler = apiVersionMiddleware(routerTransformerFunc(MirroringRouterTransformerHandler))
	}
	mux.HandleFunc("/routerTransform", b.routerTransformHandler)
	if b.routerBatchTransformHandler == nil {
		b.routerBatchTransformHandler = apiVersionMiddleware(routerBatchTransformerFunc(MirroringRouterTransformerHandler))
	}
	mux.HandleFunc("/batch", b.routerBatchTransformHandler)

	// features
	features := []byte(`{"routerTransform": {}, "supportSourceTransformV1": true, "upgradedToSourceTransformV2": true}`)
	for destType := range b.routerTransforms {
		features, _ = sjson.SetBytes(features, "routerTransform."+destType, true)
	}
	mux.HandleFunc("/features", func(w http.ResponseWriter, r *http.Request) {
		if b.featuresHandler != nil {
			b.featuresHandler(w, r)
		} else {
			_, _ = w.Write(features)
		}
	})
	return httptest.NewServer(mux)
}

func transformerFunc(h TransformerHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.Header.Get("X-Content-Format") == "json+compactedv1" {
			var ctr types.CompactedTransformRequest
			if err := jsonrs.Unmarshal(data, &ctr); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = jsonrs.NewEncoder(w).Encode(h(ctr.ToTransformerEvents()))
			return
		}
		var request []types.TransformerEvent
		if err := jsonrs.Unmarshal(data, &request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = jsonrs.NewEncoder(w).Encode(h(request))
	}
}

func srcTransformerFunc(h SrcHydrationHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var request types.SrcHydrationRequest
		if err := jsonrs.Unmarshal(data, &request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		response, err := h(request)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		_ = jsonrs.NewEncoder(w).Encode(response)
	}
}

func routerTransformerFunc(h RouterTransformerHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.Header.Get("X-Content-Format") == "json+compactedv1" {
			request, err := routerCompactedTransformMessageToTransformMessage(data)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = jsonrs.NewEncoder(w).Encode(struct {
				Output []routerTypes.DestinationJobT `json:"output"`
			}{
				Output: h(request),
			})
			return
		}
		var request routerTypes.TransformMessageT
		if err := jsonrs.Unmarshal(data, &request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		_ = jsonrs.NewEncoder(w).Encode(struct {
			Output []routerTypes.DestinationJobT `json:"output"`
		}{
			Output: h(request),
		})
	}
}

func routerBatchTransformerFunc(h RouterTransformerHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.Header.Get("X-Content-Format") == "json+compactedv1" {
			request, err := routerCompactedTransformMessageToTransformMessage(data)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = jsonrs.NewEncoder(w).Encode(h(request))
			return
		}
		var request routerTypes.TransformMessageT
		if err := jsonrs.Unmarshal(data, &request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = jsonrs.NewEncoder(w).Encode(h(request))
	}
}

func apiVersionMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("apiVersion", "2")
		next.ServeHTTP(w, r)
	})
}

func routerCompactedTransformMessageToTransformMessage(data []byte) (routerTypes.TransformMessageT, error) {
	var ctm routerTypes.CompactedTransformMessageT
	if err := jsonrs.Unmarshal(data, &ctm); err != nil {
		return routerTypes.TransformMessageT{}, err
	}
	var request routerTypes.TransformMessageT
	for _, event := range ctm.Data {
		request.Data = append(request.Data, routerTypes.RouterJobT{
			Message:     event.Message,
			JobMetadata: event.JobMetadata,
			Destination: ctm.Destinations[event.JobMetadata.DestinationID],
			Connection:  ctm.Connections[event.JobMetadata.SourceID+":"+event.JobMetadata.DestinationID],
		})
	}
	request.DestType = ctm.DestType
	return request, nil
}

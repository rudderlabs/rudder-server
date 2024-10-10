package transformertest

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/rudderlabs/rudder-server/router/types"

	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-server/processor/transformer"
)

// NewBuilder returns a new test transformer Builder
func NewBuilder() *Builder {
	b := &Builder{
		routerTransforms:      map[string]struct{}{},
		destTransformHandlers: map[string]http.HandlerFunc{},
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
	features := []byte(`{"routerTransform": {}, "supportSourceTransformV1": true}`)
	for destType := range b.routerTransforms {
		features, _ = sjson.SetBytes(features, "routerTransform."+destType, true)
	}
	mux.HandleFunc("/features", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(features)
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
		var request []transformer.TransformerEvent
		if err := json.Unmarshal(data, &request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(h(request))
	}
}

func routerTransformerFunc(h RouterTransformerHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var request types.TransformMessageT
		if err := json.Unmarshal(data, &request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		_ = json.NewEncoder(w).Encode(struct {
			Output []types.DestinationJobT `json:"output"`
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
		var request types.TransformMessageT
		if err := json.Unmarshal(data, &request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(h(request))
	}
}

func apiVersionMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("apiVersion", "2")
		next.ServeHTTP(w, r)
	})
}

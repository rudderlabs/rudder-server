package transformertest

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"

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
	routerTransforms      map[string]struct{}
	userTransformHandler  http.HandlerFunc
	destTransformHandlers map[string]http.HandlerFunc
	trackingPlanHandler   http.HandlerFunc
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

	// features
	features := []byte(`{"routerTransform": {}}`)
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
			w.WriteHeader(500)
			return
		}
		var request []transformer.TransformerEvent
		if err := json.Unmarshal(data, &request); err != nil {
			w.WriteHeader(400)
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

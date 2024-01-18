package extensions

import (
	"bytes"
	"fmt"
	"io"
	"net/http"

	"github.com/tidwall/sjson"
)

// Augmenter is an extension point for adding the appropriate authorization information to oauth requests.
type Augmenter interface {
	// Augment adds the Authorization header to the request and sets the request body.
	Augment(r *http.Request, body, token []byte) error
}

// BodyAugmenter is an Augmenter that adds the authorization information to the request body.
var BodyAugmenter = &bodyAugmenter{}

// HeaderAugmenter is an Augmenter that adds the authorization information to the request header.
var HeaderAugmenter = &headerAugmenter{}

type bodyAugmenter struct{}

// Augment adds the authorization information to the request body and sets the new request body to the request.
func (t *bodyAugmenter) Augment(r *http.Request, body, token []byte) error {
	augmentedBody, err := sjson.SetRawBytes(body, "input.0.metadata.secret", token)
	if err != nil {
		return fmt.Errorf("failed to augment request body: %w", err)
	}
	r.ContentLength = int64(len(augmentedBody))
	r.Body = io.NopCloser(bytes.NewReader(augmentedBody))
	return nil
}

type headerAugmenter struct{}

// Augment adds the Authorization header to the request and sets the request body.
func (t *headerAugmenter) Augment(r *http.Request, body, token []byte) error {
	r.Header.Set("X-Rudder-Dest-Info", string(token))
	r.Body = io.NopCloser(bytes.NewReader(body))
	return nil
}

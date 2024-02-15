package extensions

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	// oauthV2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/tidwall/sjson"
)

// Augmenter is an extension point for adding the appropriate authorization information to oauth requests.
type Augmenter interface {
	// Augment adds the Authorization header to the request and sets the request body.
	Augment(r *http.Request, body []byte, secret json.RawMessage) error
}

// BodyAugmenter is an Augmenter that adds the authorization information to the request body.
var BodyAugmenter = &bodyAugmenter{
	augmenterPath: "input.0.metadata.secret",
}

// HeaderAugmenter is an Augmenter that adds the authorization information to the request header.
var HeaderAugmenter = &headerAugmenter{}

type bodyAugmenter struct {
	augmenterPath string
}

// Overload of Earlier Augment function
func (t *bodyAugmenter) Augment(r *http.Request, body []byte, secret json.RawMessage) error {
	augmentedBody, err := sjson.SetRawBytes(body, t.augmenterPath, secret)
	if err != nil {
		return fmt.Errorf("failed to augment request body: %w", err)
	}
	r.ContentLength = int64(len(augmentedBody))
	r.Body = io.NopCloser(bytes.NewReader(augmentedBody))
	return nil
}

type headerAugmenter struct{}

// Augment adds the Authorization header to the request and sets the request body.
func (t *headerAugmenter) Augment(r *http.Request, body []byte, secret json.RawMessage) error {
	// secret, err := customFunc()
	// if err != nil {
	// 	return err
	// }
	if secret == nil {
		return fmt.Errorf("secret is nil")
	}
	r.Header.Set("X-Rudder-Dest-Info", string(secret))
	r.Body = io.NopCloser(bytes.NewReader(body))
	return nil
}

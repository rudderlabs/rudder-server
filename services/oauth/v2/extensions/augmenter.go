package extensions

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/tidwall/sjson"

	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

// Augmenter is an extension point for adding the appropriate authorization information to oauth requests.
type Augmenter interface {
	// Augment adds the Authorization header to the request and sets the request body.
	Augment(r *http.Request, body []byte, secret json.RawMessage) error
}

type bodyAugmenter struct {
	AugmenterPath string
}
type headerAugmenter struct {
	HeaderName string
}

// BodyAugmenter is an Augmenter that adds the authorization information to the request body.
var BodyAugmenter = &bodyAugmenter{
	AugmenterPath: fmt.Sprintf("input.0.metadata.%s", v2.SecretKey),
}

// HeaderAugmenter is an Augmenter that adds the authorization information to the request header.
var HeaderAugmenter = &headerAugmenter{
	HeaderName: "X-Rudder-Dest-Info",
}

// Overload of Earlier Augment function
func (t *bodyAugmenter) Augment(r *http.Request, body []byte, secret json.RawMessage) error {
	augmentedBody, err := sjson.SetRawBytes(body, t.AugmenterPath, secret)
	if err != nil {
		return fmt.Errorf("failed to augment request body: %w", err)
	}
	r.ContentLength = int64(len(augmentedBody))
	r.Body = io.NopCloser(bytes.NewReader(augmentedBody))
	return nil
}

// Augment adds the Authorization header to the request and sets the request body.
func (t *headerAugmenter) Augment(r *http.Request, body []byte, secret json.RawMessage) error {
	if secret == nil {
		return fmt.Errorf("secret is nil")
	}
	actSecret := v2.AccountSecret{
		Secret: secret,
	}
	secretJson, err := json.Marshal(actSecret)
	if err != nil {
		return err
	}
	r.Header.Set(t.HeaderName, string(secretJson))
	r.Body = io.NopCloser(bytes.NewReader(body))
	return nil
}

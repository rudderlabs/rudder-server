package extensions

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
)

// Augmenter is an extension point for adding the appropriate authorization information to oauth requests.
type Augmenter interface {
	// Augment adds the Authorization header to the request and sets the request body.
	Augment(r *http.Request, body []byte, secret json.RawMessage) error
}

type routerBodyAugmenter struct {
	AugmenterPath string
}
type headerAugmenter struct {
	HeaderName string
}

// RouterBodyAugmenter is an Augmenter that adds the authorization information to the request body.
var RouterBodyAugmenter = &routerBodyAugmenter{
	AugmenterPath: "input",
}

// HeaderAugmenter is an Augmenter that adds the authorization information to the request header.
var HeaderAugmenter = &headerAugmenter{
	HeaderName: "X-Rudder-Dest-Info",
}

// Augment adds the secret information to request body
func (t *routerBodyAugmenter) Augment(r *http.Request, body []byte, secret json.RawMessage) error {
	totalInputs := gjson.GetBytes(body, fmt.Sprintf("%s.#", t.AugmenterPath)).Int()
	augmentedBody := body
	var err error
	for i := 0; i < int(totalInputs); i++ {
		augmentedBody, err = sjson.SetRawBytes(augmentedBody, fmt.Sprintf("%s.%d.metadata.%s", t.AugmenterPath, i, common.SecretKey), secret)
		if err != nil {
			return fmt.Errorf("augmenting request body: %w", err)
		}
	}
	r.ContentLength = int64(len(augmentedBody))
	r.Body = io.NopCloser(bytes.NewReader(augmentedBody))
	return nil
}

// Augment adds secret to request header to the request and sets the request body.
func (t *headerAugmenter) Augment(r *http.Request, body []byte, secret json.RawMessage) error {
	if secret == nil {
		return errors.New("secret is nil")
	}
	actSecret := v2.AccountSecret{
		Secret: secret,
	}
	secretJson, err := json.Marshal(actSecret)
	if err != nil {
		return fmt.Errorf("marshalling secret: %w", err)
	}
	r.Header.Set(t.HeaderName, string(secretJson))
	r.Body = io.NopCloser(bytes.NewReader(body))
	return nil
}

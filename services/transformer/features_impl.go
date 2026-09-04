package transformer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync/atomic"
	"time"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/utils/httputil"
)

// featuresPayload is an immutable snapshot of a parsed /features response. A new snapshot is
// published atomically whenever the transformer returns a different body, so readers never see
// partial state and never parse JSON on the hot path.
type featuresPayload struct {
	raw json.RawMessage

	RouterTransform                        map[string]bool `json:"routerTransform"`
	TransformerProxy                       map[string]bool `json:"transformerProxy"`
	Regulations                            []string        `json:"regulations"`
	SupportSourceTransformV1               bool            `json:"supportSourceTransformV1"`
	UpgradedToSourceTransformV2            bool            `json:"upgradedToSourceTransformV2"`
	SupportTransformerProxyV1              bool            `json:"supportTransformerProxyV1"`
	SupportDestTransformCompactedPayloadV1 bool            `json:"supportDestTransformCompactedPayloadV1"`
}

// sourceTransformerVersion resolves the source transformer version advertised by the snapshot,
// panicking if the transformer only speaks the deprecated v0 protocol.
func (f *featuresPayload) sourceTransformerVersion() string {
	if f.UpgradedToSourceTransformV2 {
		return V2
	}
	if f.SupportSourceTransformV1 {
		return V1
	}
	panic("Webhook source v0 version has been deprecated. This is a breaking change. Upgrade transformer version to greater than 1.50.0 for v1")
}

func parseFeatures(body []byte) (*featuresPayload, error) {
	var f featuresPayload
	if err := jsonrs.Unmarshal(body, &f); err != nil {
		return nil, err
	}
	f.raw = body
	return &f, nil
}

type featuresService struct {
	logger   logger.Logger
	waitChan chan struct{}
	options  FeaturesServiceOptions
	features atomic.Pointer[featuresPayload]
	client   *http.Client
}

func (t *featuresService) isInitialized() bool {
	select {
	case <-t.waitChan:
		return true
	default:
		return false
	}
}

func (t *featuresService) SourceTransformerVersion() string {
	// Before the first successful fetch the snapshot holds hardcoded defaults; report v2 rather
	// than trusting them. The v0 deprecation check runs against every fetched snapshot before it
	// is published, so by the time Wait() releases callers a deprecated transformer has already
	// caused a panic.
	if !t.isInitialized() {
		return V2
	}
	return t.features.Load().sourceTransformerVersion()
}

func (t *featuresService) TransformerProxyVersion() string {
	if t.features.Load().SupportTransformerProxyV1 {
		return V1
	}
	return V0
}

func (t *featuresService) RouterTransform(destType string) bool {
	return t.features.Load().RouterTransform[destType]
}

// TransformerProxy reports whether the transformer declares destType deliverable via the proxy.
// Absent from older transformer images, in which case this is false and the caller falls back to
// the Router.<DEST>.transformerProxy config.
func (t *featuresService) TransformerProxy(destType string) bool {
	return t.features.Load().TransformerProxy[destType]
}

func (t *featuresService) Regulations() []string {
	if regulations := t.features.Load().Regulations; regulations != nil {
		return slices.Clone(regulations)
	}
	return []string{}
}

// SupportDestTransformCompactedPayloadV1 checks if the transformer supports compacted payload for destination transformation
func (t *featuresService) SupportDestTransformCompactedPayloadV1() bool {
	return t.features.Load().SupportDestTransformCompactedPayloadV1
}

func (t *featuresService) Wait() chan struct{} {
	return t.waitChan
}

func (t *featuresService) syncTransformerFeatureJson(ctx context.Context) {
	var initDone bool
	t.logger.Infon("Fetching transformer features", logger.NewStringField("transformerURL", t.options.TransformerURL))
	for {
		if t.fetchWithRetries(ctx) && !initDone {
			initDone = true
			t.logger.Infon("Fetched transformer features", logger.NewStringField("transformerURL", t.options.TransformerURL))
			close(t.waitChan)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(t.options.PollInterval):
		}
	}
}

func (t *featuresService) fetchWithRetries(ctx context.Context) bool {
	for i := 0; i < t.options.FeaturesRetryMaxAttempts; i++ {
		if ctx.Err() != nil {
			return false
		}
		err := t.makeFeaturesFetchCall()
		if err == nil {
			return true
		}
		t.logger.Errorn("Error fetching transformer features",
			logger.NewStringField("transformerURL", t.options.TransformerURL),
			obskit.Error(err),
		)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(2 * time.Millisecond):
		}
	}
	return false
}

func (t *featuresService) makeFeaturesFetchCall() error {
	url := t.options.TransformerURL + "/features"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	res, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}

	defer func() { httputil.CloseResponse(res) }()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %w", err)
	}

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected response status: %s", res.Status)
	}

	if bytes.Equal(t.features.Load().raw, body) {
		return nil
	}
	features, err := parseFeatures(body)
	if err != nil {
		return fmt.Errorf("parsing features: %w", err)
	}
	// The v0 deprecation check must fire before the snapshot is published and before Wait()
	// releases the server components.
	_ = features.sourceTransformerVersion()
	t.features.Store(features)
	return nil
}

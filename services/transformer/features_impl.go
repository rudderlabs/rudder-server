package transformer

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

type featuresService struct {
	logger   logger.Logger
	waitChan chan struct{}
	options  FeaturesServiceOptions
	features json.RawMessage
	client   *http.Client
}

func (t *featuresService) SourceTransformerVersion() string {
	// If transformer is upgraded to V2, enable V2 spec communication
	if gjson.GetBytes(t.features, "upgradedToSourceTransformV2").Bool() {
		return V2
	}

	// V0 Deprecation: This function will verify if `supportSourceTransformV1` is available and enabled
	// if `supportSourceTransformV1` is not enabled, transformer is not compatible and server will panic with appropriate message.
	if gjson.GetBytes(t.features, "supportSourceTransformV1").Bool() {
		return V1
	}
	panic("Webhook source v0 version has been deprecated. This is a breaking change. Upgrade transformer version to greater than 1.50.0 for v1")
}

func (t *featuresService) TransformerProxyVersion() string {
	if gjson.GetBytes(t.features, "supportTransformerProxyV1").Bool() {
		return V1
	}

	return V0
}

func (t *featuresService) RouterTransform(destType string) bool {
	return gjson.GetBytes(t.features, "routerTransform."+destType).Bool()
}

func (t *featuresService) Regulations() []string {
	regulationFeatures := gjson.GetBytes(t.features, "regulations")
	if regulationFeatures.Exists() && regulationFeatures.IsArray() {
		return lo.Map(regulationFeatures.Array(), func(f gjson.Result, _ int) string {
			return f.String()
		})
	}
	return []string{}
}

func (t *featuresService) Wait() chan struct{} {
	return t.waitChan
}

func (t *featuresService) syncTransformerFeatureJson(ctx context.Context) {
	var initDone bool
	t.logger.Infof("Fetching transformer features from %s", t.options.TransformerURL)
	for {
		var downloaded bool
		for i := 0; i < t.options.FeaturesRetryMaxAttempts; i++ {

			if ctx.Err() != nil {
				return
			}

			retry := t.makeFeaturesFetchCall()
			if retry {
				t.logger.Infof("Fetched transformer features from %s (retry: %v)", t.options.TransformerURL, retry)
				select {
				case <-ctx.Done():
					return
				case <-time.After(2 * time.Millisecond):
					continue
				}
			}
			downloaded = true
			break
		}

		if downloaded && !initDone {
			initDone = true
			close(t.waitChan)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(t.options.PollInterval):
		}
	}
}

func (t *featuresService) makeFeaturesFetchCall() bool {
	url := t.options.TransformerURL + "/features"
	req, err := http.NewRequest("GET", url, bytes.NewReader([]byte{}))
	if err != nil {
		t.logger.Error("error creating request - ", err)
		return true
	}
	res, err := t.client.Do(req)
	if err != nil {
		t.logger.Error("error sending request - ", err)
		return true
	}

	defer func() { httputil.CloseResponse(res) }()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return true
	}

	if res.StatusCode == 200 {
		t.features = body

		//  we are calling this to see if the transformer version is deprecated. if so, we panic.
		t.SourceTransformerVersion()
	} else if res.StatusCode == 404 {
		t.features = json.RawMessage(defaultTransformerFeatures)
	}

	return false
}

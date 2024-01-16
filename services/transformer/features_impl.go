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

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

type featuresService struct {
	logger   logger.Logger
	waitChan chan struct{}
	config   FeaturesServiceConfig
	features json.RawMessage
}

func (t *featuresService) SourceTransformerVersion() string {
	if gjson.GetBytes(t.features, "supportSourceTransformV1").Bool() {
		return V1
	}

	return V0
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
	t.logger.Infof("Fetching transformer features from %s", t.config.TransformerURL)
	for {
		var downloaded bool
		for i := 0; i < t.config.FeaturesRetryMaxAttempts; i++ {

			if ctx.Err() != nil {
				return
			}

			retry := t.makeFeaturesFetchCall()
			if retry {
				t.logger.Infof("Fetched transformer features from %s (retry: %v)", t.config.TransformerURL, retry)
				select {
				case <-ctx.Done():
					return
				case <-time.After(200 * time.Millisecond):
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
		case <-time.After(t.config.PollInterval):
		}
	}
}

func (t *featuresService) makeFeaturesFetchCall() bool {
	url := t.config.TransformerURL + "/features"
	req, err := http.NewRequest("GET", url, bytes.NewReader([]byte{}))
	if err != nil {
		t.logger.Error("error creating request - ", err)
		return true
	}
	tr := &http.Transport{}
	client := &http.Client{Transport: tr, Timeout: config.GetDuration("HttpClient.processor.timeout", 30, time.Second)}
	res, err := client.Do(req)
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
	} else if res.StatusCode == 404 {
		t.features = json.RawMessage(defaultTransformerFeatures)
	}

	return false
}

package features

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type FeatureFlags interface {
	Register(name string, featureList []string) error
}

type FeatureFlagsImpl struct {
	Context context.Context
}

type Feature struct {
	Name     string   `json:"name"`
	Features []string `json:"features"`
}

type FeatureComponents struct {
	Components []Feature `json:"components"`
}

func NewFeatureFlags(context context.Context) FeatureFlags {
	return &FeatureFlagsImpl{Context: context}
}

func (f *FeatureFlagsImpl) Register(name string, featureList []string) error {
	configBackendURL := config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
	url := fmt.Sprintf("%s/data-plane/namespaces/%s/settings", configBackendURL, config.GetNamespaceIdentifier())

	payload, err := registerPayload(name, featureList)
	if err != nil {
		return fmt.Errorf("failed to construct register payload with error: %s", err.Error())
	}

	_, statusCode := misc.HTTPCallWithRetryWithTimeout(url, payload, 60)
	if statusCode != 200 {
		return fmt.Errorf("failed to register features for: %s with featuresList: %v", name, featureList)
	}
	return nil
}

func registerPayload(name string, featureList []string) ([]byte, error) {
	if len(featureList) == 0 {
		return nil, errors.New("no featuresList are provided to register")
	}

	feature := Feature{
		Name:     name,
		Features: featureList,
	}
	featureComponent := FeatureComponents{[]Feature{feature}}

	return json.Marshal(featureComponent)
}

package configenv

import (
	"os"
	"reflect"
	"strings"

	"github.com/jeremywohl/flatten"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

type HandleT struct {
	Log logger.Logger
}

var configEnvReplacer string

func loadConfig() {
	configEnvReplacer = config.GetString("BackendConfig.configEnvReplacer", "env.")
}

// ReplaceConfigWithEnvVariables : Replaces all env variables in the config
func (h *HandleT) ReplaceConfigWithEnvVariables(workspaceConfig []byte) (updatedConfig []byte) {
	configMap := make(map[string]interface{}, 0)

	err := jsonrs.Unmarshal(workspaceConfig, &configMap)
	if err != nil {
		h.Log.Errorn("[ConfigEnv] Error while parsing request",
			obskit.Error(err),
			logger.NewStringField("workspaceConfig", string(workspaceConfig)),
		)
		return workspaceConfig
	}

	flattenedConfig, err := flatten.Flatten(configMap, "", flatten.DotStyle)
	if err != nil {
		h.Log.Errorn("[ConfigEnv] Failed to flatten workspace config",
			obskit.Error(err),
		)
		return workspaceConfig
	}

	for configKey, v := range flattenedConfig {
		reflectType := reflect.TypeOf(v)
		if reflectType != nil && reflectType.String() == "string" {
			valString := v.(string)
			shouldReplace := strings.HasPrefix(strings.TrimSpace(valString), configEnvReplacer)
			if shouldReplace {
				envVariable := valString[len(configEnvReplacer):]
				envVarValue := os.Getenv(envVariable)
				if envVarValue == "" {
					h.Log.Errorn("[ConfigEnv] Missing envVariable. Either set it as envVariable or remove configEnvReplacer from the destination config.",
						logger.NewStringField("envVariable", envVariable),
						logger.NewStringField("configEnvReplacer", configEnvReplacer),
					)
					continue
				}
				workspaceConfig, err = sjson.SetBytes(workspaceConfig, configKey, envVarValue)
				if err != nil {
					h.Log.Errorn("[ConfigEnv] Failed to set config",
						logger.NewStringField("configKey", configKey),
						obskit.Error(err),
					)
				}
			}
		}
	}

	return workspaceConfig
}

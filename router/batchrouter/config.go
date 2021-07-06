package batchrouter

import (
	"github.com/rudderlabs/rudder-server/config"
)

func getBatchRouterConfigInt(key string, destType string, defaultValue int) int {
	destOverrideFound := config.IsSet("BatchRouter." + destType + "." + key)
	if destOverrideFound {
		return config.GetInt("BatchRouter."+destType+"."+key, defaultValue)
	} else {
		return config.GetInt("BatchRouter."+key, defaultValue)
	}
}

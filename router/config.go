package router

import (
	"time"

	"github.com/rudderlabs/rudder-server/config"
)

func getRouterConfigBool(key string, destType string, defaultValue bool) bool {

	destOverrideFound := config.IsSet("router." + destType + "." + key)
	if destOverrideFound {
		return config.GetBool("router."+destType+"."+key, defaultValue)
	} else {
		return config.GetBool("router."+key, defaultValue)
	}
}

func getRouterConfigInt(key string, destType string, defaultValue int) int {

	destOverrideFound := config.IsSet("router." + destType + "." + key)
	if destOverrideFound {
		return config.GetInt("router."+destType+"."+key, defaultValue)
	} else {
		return config.GetInt("router."+key, defaultValue)
	}
}

func getRouterConfigInt64(key string, destType string, defaultValue int64) int64 {

	destOverrideFound := config.IsSet("router." + destType + "." + key)
	if destOverrideFound {
		return config.GetInt64("router."+destType+"."+key, defaultValue)
	} else {
		return config.GetInt64("router."+key, defaultValue)
	}
}

func getRouterConfigFloat64(key string, destType string, defaultValue float64) float64 {

	destOverrideFound := config.IsSet("router." + destType + "." + key)
	if destOverrideFound {
		return config.GetFloat64("router."+destType+"."+key, defaultValue)
	} else {
		return config.GetFloat64("router."+key, defaultValue)
	}
}

func getRouterConfigString(key string, destType string, defaultValue string) string {

	destOverrideFound := config.IsSet("router." + destType + "." + key)
	if destOverrideFound {
		return config.GetString("router."+destType+"."+key, defaultValue)
	} else {
		return config.GetString("router."+key, defaultValue)
	}
}

func getRouterConfigDuration(key string, destType string, defaultValue time.Duration) time.Duration {

	destOverrideFound := config.IsSet("router." + destType + "." + key)
	if destOverrideFound {
		return config.GetDuration("router."+destType+"."+key, defaultValue)
	} else {
		return config.GetDuration("router."+key, defaultValue)
	}
}

package router

import (
	"github.com/rudderlabs/rudder-server/config"
)

func getRouterConfigBool(key, destType string, defaultValue bool) bool {
	destOverrideFound := config.IsSet("Router." + destType + "." + key)
	if destOverrideFound {
		return config.GetBool("Router."+destType+"."+key, defaultValue)
	} else {
		return config.GetBool("Router."+key, defaultValue)
	}
}

func getRouterConfigInt(key, destType string, defaultValue int) int {
	destOverrideFound := config.IsSet("Router." + destType + "." + key)
	if destOverrideFound {
		return config.GetInt("Router."+destType+"."+key, defaultValue)
	} else {
		return config.GetInt("Router."+key, defaultValue)
	}
}

// skipcq: SCC-U1000
func getRouterConfigInt64(key, destType string, defaultValue int64) int64 {
	destOverrideFound := config.IsSet("Router." + destType + "." + key)
	if destOverrideFound {
		return config.GetInt64("Router."+destType+"."+key, defaultValue)
	} else {
		return config.GetInt64("Router."+key, defaultValue)
	}
}

// skipcq: SCC-U1000
func getRouterConfigFloat64(key, destType string, defaultValue float64) float64 {
	destOverrideFound := config.IsSet("Router." + destType + "." + key)
	if destOverrideFound {
		return config.GetFloat64("Router."+destType+"."+key, defaultValue)
	} else {
		return config.GetFloat64("Router."+key, defaultValue)
	}
}

// skipcq: SCC-U1000
func getRouterConfigString(key, destType, defaultValue string) string {
	destOverrideFound := config.IsSet("Router." + destType + "." + key)
	if destOverrideFound {
		return config.GetString("Router."+destType+"."+key, defaultValue)
	} else {
		return config.GetString("Router."+key, defaultValue)
	}
}

package router

import (
	"github.com/rudderlabs/rudder-go-kit/config"
)

func getRouterConfigBool(key, destType string, defaultValue bool) bool {
	return config.GetBoolVar(defaultValue, getRouterConfigKeys(key, destType)...)
}

func getRouterConfigInt(key, destType string, defaultValue int) int {
	return config.GetIntVar(defaultValue, 1, getRouterConfigKeys(key, destType)...)
}

func getHierarchicalRouterConfigInt(destType string, defaultValue int, keys ...string) int {
	orderedKeys := make([]string, 0, len(keys)*2)
	for i := range keys {
		orderedKeys = append(orderedKeys, "Router."+destType+"."+keys[i])
		orderedKeys = append(orderedKeys, "Router."+keys[i])
	}
	return config.GetIntVar(defaultValue, 1, orderedKeys...)
}

func getReloadableRouterConfigInt(key, destType string, defaultValue int) config.ValueLoader[int] {
	return config.GetReloadableIntVar(defaultValue, 1, getRouterConfigKeys(key, destType)...)
}

func getRouterConfigKeys(key, destType string) []string {
	return []string{"Router." + destType + "." + key, "Router." + key}
}

package config

// TODO: everything in this file should be either removed or unexported
import (
	"os"
	"strings"
)

// ConfigKeyToEnv gets the env variable name from a given config key
func ConfigKeyToEnv(s string) string {
	if upperCaseMatch.MatchString(s) {
		return s
	}
	snake := camelCaseMatch.ReplaceAllString(s, "${1}_${2}")
	return "RSERVER_" + strings.ToUpper(strings.ReplaceAll(snake, ".", "_"))
}

// getEnv returns the environment value stored in key variable
func getEnv(key, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

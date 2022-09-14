package config

// TODO: everything in this file should be either removed or unexported
import (
	"os"
	"strconv"
	"strings"
)

// ConfigKeyToEnvRudder as package method to get the formatted env from a given string
func ConfigKeyToEnvRudder(s string) string {
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

// getEnvAsBool returns the boolean environment value stored in key variable
func getEnvAsBool(name string, defaultVal bool) bool {
	valueStr := getEnv(name, "")
	if value, err := strconv.ParseBool(valueStr); err == nil {
		return value
	}

	return defaultVal
}

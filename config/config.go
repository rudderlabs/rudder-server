package config

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/viper"
)

// Initialize initializes the config
func Initialize() {
	configPath := GetEnv("CONFIG_PATH", "./config.toml")

	viper.SetConfigFile(configPath)
	viper.AddConfigPath(".")
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
}

// GetInt is wrapper for viper's GetInt
func GetInt(key string) int {
	return viper.GetInt(key)
}

// GetFloat64 is wrapper for viper's GetFloat64
func GetFloat64(key string) float64 {
	return viper.GetFloat64(key)
}

// GetString is wrapper for viper's GetString
func GetString(key string) string {
	return viper.GetString(key)
}

// GetDuration is wrapper for viper's GetDuration
func GetDuration(key string) time.Duration {
	return viper.GetDuration(key)
}

// GetEnv returns the environment value stored in key variable
func GetEnv(key string, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

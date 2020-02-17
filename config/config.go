package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

// Initialize initializes the config
func Initialize() {
	if err := godotenv.Load(); err != nil {
		fmt.Println("ERROR: No .env file found")
	}
	configPath := GetEnv("CONFIG_PATH", "./config/config.toml")

	viper.SetConfigFile(configPath)
	viper.AddConfigPath(".")
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Using the default config values when config.toml is not found
		fmt.Println("Config toml file not found. Using the default values")
	}
}

//GetBool is a wrapper for viper's GetBool
func GetBool(key string, defaultValue bool) bool {
	if !viper.IsSet(key) {
		return defaultValue
	}
	return viper.GetBool(key)
}

// GetInt is wrapper for viper's GetInt
func GetInt(key string, defaultValue int) int {
	if !viper.IsSet(key) {
		return defaultValue
	}
	return viper.GetInt(key)
}

// GetInt64 is wrapper for viper's GetInt
func GetInt64(key string, defaultValue int64) int64 {
	if !viper.IsSet(key) {
		return defaultValue
	}
	return viper.GetInt64(key)
}

// GetFloat64 is wrapper for viper's GetFloat64
func GetFloat64(key string, defaultValue float64) float64 {
	if !viper.IsSet(key) {
		return defaultValue
	}
	return viper.GetFloat64(key)
}

// GetString is wrapper for viper's GetString
func GetString(key string, defaultValue string) string {
	if !viper.IsSet(key) {
		return defaultValue
	}
	return viper.GetString(key)
}

// GetDuration is wrapper for viper's GetDuration
func GetDuration(key string, defaultValue time.Duration) time.Duration {
	if !viper.IsSet(key) {
		return defaultValue
	}
	return viper.GetDuration(key)
}

// GetEnv returns the environment value stored in key variable
func GetEnv(key string, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

// GetEnvAsBool returns the boolean environment value stored in key variable
func GetEnvAsBool(name string, defaultVal bool) bool {
	valueStr := GetEnv(name, "")
	if value, err := strconv.ParseBool(valueStr); err == nil {
		return value
	}

	return defaultVal
}

// GetRequiredEnv returns the environment value stored in key variable, no default
func GetRequiredEnv(key string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	panic(fmt.Errorf("Fatal error, no required environment variable: %s", key))
}

// Override Config by application or command line

// SetBool override existing config
func SetBool(key string, value bool) {
	viper.Set(key, value)
}

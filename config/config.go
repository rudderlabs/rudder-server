package config

import (
	"fmt"
	"os"

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

func GetEnv(key string, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

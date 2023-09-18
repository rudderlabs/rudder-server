package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

var initialized = false

func SetEnvFile(path string) {
}

func getServerDirPath() string {
	envServerDirPath, ok := os.LookupEnv(RudderServerPathKey)

	if !ok {
		return "."
	}
	return envServerDirPath
}

func getEnvFilePath() string {
	envFilePath, ok := os.LookupEnv(RudderEnvFilePathKey)
	if !ok {
		return fmt.Sprintf("%s/%s", getServerDirPath(), DefaultEnvFile)
	}
	return envFilePath
}

func GetEnv(key string) string {
	if !initialized {
		// Load the .env file, if it exists
		_ = godotenv.Load(getEnvFilePath())
		initialized = true
	}

	return os.Getenv(key)
}

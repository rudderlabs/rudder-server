package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/cast"
	"github.com/spf13/viper"
)

var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

var (
	whSchemaVersion string
)

// Rudder server supported config constants
const (
	EmbeddedMode      = "embedded"
	MasterMode        = "master"
	MasterSlaveMode   = "master_and_slave"
	SlaveMode         = "slave"
	OffMode           = "off"
	PooledWHSlaveMode = "embedded_master"
)

//TransformKey as package method to get the formatted env from a give string
func TransformKey(s string) string {
	snake := matchAllCap.ReplaceAllString(s, "${1}_${2}")
	snake = strings.ReplaceAll(snake, ".", "_")
	return "RSERVER_" + strings.ToUpper(snake)
}

// Initialize used to initialize config package
// Deprecated - There is no need to directly call Initialize, config is initialized via its package init()
func Initialize() {
}

func init() {
	if err := godotenv.Load(); err != nil {
		fmt.Println("INFO: No .env file found.")
	}

	configPath := GetEnv("CONFIG_PATH", "./config/config.toml")

	viper.SetConfigFile(configPath)
	viper.AddConfigPath(".")
	err := viper.ReadInConfig() // Find and read the config file
	// Don't panic if config.toml is not found or error with parsing. Use the default config values instead
	if err != nil {
		fmt.Println("[Config] :: Failed to parse Config toml, using default values:", err)
	}
}

//GetBool is a wrapper for viper's GetBool
func GetBool(key string, defaultValue bool) (value bool) {

	envVal := GetEnv(TransformKey(key), "")
	if envVal != "" {
		return cast.ToBool(envVal)
	}

	if !viper.IsSet(key) {
		return defaultValue
	}
	return viper.GetBool(key)
}

// GetInt is wrapper for viper's GetInt
func GetInt(key string, defaultValue int) (value int) {

	envVal := GetEnv(TransformKey(key), "")
	if envVal != "" {
		return cast.ToInt(envVal)
	}

	if !viper.IsSet(key) {
		return defaultValue
	}
	return viper.GetInt(key)
}

// GetInt64 is wrapper for viper's GetInt
func GetInt64(key string, defaultValue int64) (value int64) {

	envVal := GetEnv(TransformKey(key), "")
	if envVal != "" {
		return cast.ToInt64(envVal)
	}

	if !viper.IsSet(key) {
		return defaultValue
	}
	return viper.GetInt64(key)
}

// GetFloat64 is wrapper for viper's GetFloat64
func GetFloat64(key string, defaultValue float64) (value float64) {

	envVal := GetEnv(TransformKey(key), "")
	if envVal != "" {
		return cast.ToFloat64(envVal)
	}

	if !viper.IsSet(key) {
		return defaultValue
	}
	return viper.GetFloat64(key)
}

// GetString is wrapper for viper's GetString
func GetString(key string, defaultValue string) (value string) {

	envVal := GetEnv(TransformKey(key), "")
	if envVal != "" {
		return envVal
	}

	if !viper.IsSet(key) {
		return defaultValue
	}
	return viper.GetString(key)
}

// GetDuration is wrapper for viper's GetDuration
func GetDuration(key string, defaultValue time.Duration) (value time.Duration) {

	envVal := GetEnv(TransformKey(key), "")
	if envVal != "" {
		return cast.ToDuration(envVal)
	}

	if !viper.IsSet(key) {
		return defaultValue
	}
	return viper.GetDuration(key)
}

// IsSet checks if config is set for a key
func IsSet(key string) bool {
	if _, exists := os.LookupEnv(TransformKey(key)); exists {
		return true
	}

	return viper.IsSet(key)
}

// IsEnvSet checks if an environment variable is set
func IsEnvSet(key string) bool {
	_, exists := os.LookupEnv(key)
	return exists
}

// GetEnv returns the environment value stored in key variable
func GetEnv(key string, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

// GetEnvAsInt returns the int value of environment value stored in the key variable
// If not set, default value will be return. If set but unparsable, returns 0
func GetEnvAsInt(key string, defaultVal int) int {
	stringValue, exists := os.LookupEnv(key)
	if !exists {
		return defaultVal
	}
	if value, err := strconv.Atoi(stringValue); err == nil {
		return value
	}
	return 0
}

// GetRequiredEnvAsInt returns the environment value stored in key variable as int, no default
func GetRequiredEnvAsInt(key string) int {
	stringValue, exists := os.LookupEnv(key)
	if !exists {
		panic(fmt.Errorf("Fatal error, no required environment variable: %s", key))
	}
	if value, err := strconv.Atoi(stringValue); err == nil {
		return value
	}
	panic(fmt.Sprintf("Unable to parse the value of %s env variable. Value : %s", key, stringValue))
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

func SetString(key string, value string) {
	viper.Set(key, value)
}

//GetWorkspaceToken returns the workspace token provided in the environment variables
//Env variable CONFIG_BACKEND_TOKEN is deprecating soon
//WORKSPACE_TOKEN is newly introduced. This will override CONFIG_BACKEND_TOKEN
func GetWorkspaceToken() string {
	token := GetEnv("WORKSPACE_TOKEN", "")
	if token != "" && token != "<your_token_here>" {
		return token
	}

	return GetEnv("CONFIG_BACKEND_TOKEN", "")
}

// returns value stored in KUBE_NAMESPACE env var
func GetKubeNamespace() string {
	return GetEnv("KUBE_NAMESPACE", "")
}

func SetWHSchemaVersion(version string) {
	whSchemaVersion = version
}

func GetWHSchemaVersion() string {
	return whSchemaVersion
}

func GetVarCharMaxForRS() bool {
	return GetBool("Warehouse.redshift.setVarCharMax", false)
}

package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/joho/godotenv"
	"github.com/spf13/cast"
	"github.com/spf13/viper"
)

var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

var (
	whSchemaVersion        string
	hotReloadableConfig    map[string]*ConfigVar
	nonHotReloadableConfig map[string]*ConfigVar
	configVarLock          sync.RWMutex
)

type ConfigVar struct {
	value           interface{}
	multiplier      interface{}
	isHotReloadable bool
	defaultValue    interface{}
	keys            []string
}

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

func Load() {
	if err := godotenv.Load(); err != nil {
		fmt.Println("INFO: No .env file found.")
	}
	hotReloadableConfig = make(map[string]*ConfigVar)
	nonHotReloadableConfig = make(map[string]*ConfigVar)
	configPath := GetEnv("CONFIG_PATH", "./config/config.yaml")
	viper.SetConfigFile(configPath)
	err := viper.ReadInConfig() // Find and read the config file
	UpdateConfig()
	// Don't panic if config.yaml is not found or error with parsing. Use the default config values instead
	if err != nil {
		fmt.Println("[Config] :: Failed to parse Config Yaml, using default values:", err)
	}
}

func UpdateConfig() {
	viper.WatchConfig()
	viper.OnConfigChange(func(e fsnotify.Event) {
		watchForConfigChange()
	})
}

func watchForConfigChange() {
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("failed while trying to update Config Variabled with Error: %w", r)
			fmt.Println(err)
		}
	}()
	configVarLock.RLock()
	defer configVarLock.RUnlock()
	_ = checkAndHotReloadConfig(hotReloadableConfig)
	isChanged := checkAndHotReloadConfig(nonHotReloadableConfig)
	if isChanged && GetEnvAsBool("RESTART_ON_CONFIG_CHANGE", false) {
		os.Exit(1)
	}
}

func checkAndHotReloadConfig(configMap map[string]*ConfigVar) (hasConfigChanged bool) {
	for key, configVal := range configMap {
		value := configVal.value
		switch value := value.(type) {
		case *int:
			var _value int
			var isSet bool
			envVal := GetEnv(TransformKey(key), "")
			if envVal != "" {
				isSet = true
				_value = cast.ToInt(envVal)
			} else {
				for _, key := range configVal.keys {
					if viper.IsSet(key) {
						isSet = true
						_value = GetInt(key, configVal.defaultValue.(int))
						break
					}
				}
			}
			if !isSet {
				_value = configVal.defaultValue.(int)
			}
			_value = _value * configVal.multiplier.(int)
			if _value != *value {
				hasConfigChanged = true
				if configVal.isHotReloadable {
					fmt.Printf("The value of %s changed from %d to %d\n", key, *value, _value)
					*value = _value
				}
			}
		case *int64:
			var _value int64
			var isSet bool
			envVal := GetEnv(TransformKey(key), "")
			if envVal != "" {
				isSet = true
				_value = cast.ToInt64(envVal)
			} else {
				for _, key := range configVal.keys {
					if viper.IsSet(key) {
						isSet = true
						_value = GetInt64(key, configVal.defaultValue.(int64))
						break
					}
				}
			}
			if !isSet {
				_value = configVal.defaultValue.(int64)
			}
			_value = _value * configVal.multiplier.(int64)
			if _value != *value {
				hasConfigChanged = true
				if configVal.isHotReloadable {
					fmt.Printf("The value of %s changed from %d to %d\n", key, *value, _value)
					*value = _value
				}
			}
		case *string:
			var _value string
			var isSet bool
			envVal := GetEnv(TransformKey(key), "")
			if envVal != "" {
				isSet = true
				_value = cast.ToString(envVal)
			} else {
				for _, key := range configVal.keys {
					if viper.IsSet(key) {
						isSet = true
						_value = GetString(key, configVal.defaultValue.(string))
						break
					}
				}
			}
			if !isSet {
				_value = configVal.defaultValue.(string)
			}
			if _value != *value {
				hasConfigChanged = true
				if configVal.isHotReloadable {
					fmt.Printf("The value of %s changed from %v to %v\n", key, *value, _value)
					*value = _value
				}
			}
		case *time.Duration:
			var _value time.Duration
			var isSet bool
			envVal := GetEnv(TransformKey(key), "")
			if envVal != "" {
				isSet = true
				_value = cast.ToDuration(envVal)
			} else {
				for _, key := range configVal.keys {
					if viper.IsSet(key) {
						isSet = true
						_value = GetDuration(key, configVal.defaultValue.(time.Duration), configVal.multiplier.(time.Duration))
						break
					}
				}
			}
			if !isSet {
				_value = configVal.defaultValue.(time.Duration) * configVal.multiplier.(time.Duration)
			}
			if _value != *value {
				hasConfigChanged = true
				if configVal.isHotReloadable {
					fmt.Printf("The value of %s changed from %v to %v\n", key, *value, _value)
					*value = _value
				}
			}
		case *bool:
			var _value bool
			var isSet bool
			envVal := GetEnv(TransformKey(key), "")
			if envVal != "" {
				isSet = true
				_value = cast.ToBool(envVal)
			} else {
				for _, key := range configVal.keys {
					if viper.IsSet(key) {
						isSet = true
						_value = GetBool(key, configVal.defaultValue.(bool))
						break
					}
				}
			}
			if !isSet {
				_value = configVal.defaultValue.(bool)
			}
			if _value != *value {
				hasConfigChanged = true
				if configVal.isHotReloadable {
					fmt.Printf("The value of %s changed from %v to %v\n", key, *value, _value)
					*value = _value
				}
			}
		case *float64:
			var _value float64
			var isSet bool
			envVal := GetEnv(TransformKey(key), "")
			if envVal != "" {
				isSet = true
				_value = cast.ToFloat64(envVal)
			} else {
				for _, key := range configVal.keys {
					if viper.IsSet(key) {
						isSet = true
						_value = GetFloat64(key, configVal.defaultValue.(float64))
						break
					}
				}
			}
			if !isSet {
				_value = configVal.defaultValue.(float64)
			}
			_value = _value * configVal.multiplier.(float64)
			if _value != *value {
				hasConfigChanged = true
				if configVal.isHotReloadable {
					fmt.Printf("The value of %s changed from %v to %v\n", key, *value, _value)
					*value = _value
				}
			}
		}
	}
	return hasConfigChanged
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

func RegisterIntConfigVariable(defaultValue int, ptr *int, isHotReloadable bool, valueScale int, keys ...string) {
	configVarLock.Lock()
	defer configVarLock.Unlock()
	var isSet bool
	configVar := ConfigVar{
		value:           ptr,
		multiplier:      valueScale,
		isHotReloadable: isHotReloadable,
		defaultValue:    defaultValue,
		keys:            keys,
	}
	if isHotReloadable {
		hotReloadableConfig[keys[0]] = &configVar
	} else {
		nonHotReloadableConfig[keys[0]] = &configVar
	}
	for _, key := range keys {
		if IsSet(key) {
			isSet = true
			*ptr = GetInt(key, defaultValue) * valueScale
			break
		}
	}
	if !isSet {
		*ptr = defaultValue * valueScale
	}
}

func RegisterBoolConfigVariable(defaultValue bool, ptr *bool, isHotReloadable bool, keys ...string) {
	configVarLock.Lock()
	defer configVarLock.Unlock()
	var isSet bool
	configVar := ConfigVar{
		value:           ptr,
		isHotReloadable: isHotReloadable,
		defaultValue:    defaultValue,
		keys:            keys,
	}
	if isHotReloadable {
		hotReloadableConfig[keys[0]] = &configVar
	} else {
		nonHotReloadableConfig[keys[0]] = &configVar
	}
	for _, key := range keys {
		if IsSet(key) {
			isSet = true
			*ptr = GetBool(key, defaultValue)
			break
		}
	}
	if !isSet {
		*ptr = defaultValue
	}
}

func RegisterFloat64ConfigVariable(defaultValue float64, ptr *float64, isHotReloadable bool, keys ...string) {
	configVarLock.Lock()
	defer configVarLock.Unlock()
	var isSet bool
	configVar := ConfigVar{
		value:           ptr,
		multiplier:      1.0,
		isHotReloadable: isHotReloadable,
		defaultValue:    defaultValue,
		keys:            keys,
	}
	if isHotReloadable {
		hotReloadableConfig[keys[0]] = &configVar
	} else {
		nonHotReloadableConfig[keys[0]] = &configVar
	}
	for _, key := range keys {
		if IsSet(key) {
			isSet = true
			*ptr = GetFloat64(key, defaultValue)
			break
		}
	}
	if !isSet {
		*ptr = defaultValue
	}
}

func RegisterInt64ConfigVariable(defaultValue int64, ptr *int64, isHotReloadable bool, valueScale int64, keys ...string) {
	configVarLock.Lock()
	defer configVarLock.Unlock()
	var isSet bool
	configVar := ConfigVar{
		value:           ptr,
		multiplier:      valueScale,
		isHotReloadable: isHotReloadable,
		defaultValue:    defaultValue,
		keys:            keys,
	}
	if isHotReloadable {
		hotReloadableConfig[keys[0]] = &configVar
	} else {
		nonHotReloadableConfig[keys[0]] = &configVar
	}
	for _, key := range keys {
		if IsSet(key) {
			isSet = true
			*ptr = GetInt64(key, defaultValue) * valueScale
			break
		}
	}
	if !isSet {
		*ptr = defaultValue * valueScale
	}
}

func RegisterDurationConfigVariable(defaultValue time.Duration, ptr *time.Duration, isHotReloadable bool, timeScale time.Duration, keys ...string) {
	configVarLock.Lock()
	defer configVarLock.Unlock()
	var isSet bool
	configVar := ConfigVar{
		value:           ptr,
		multiplier:      timeScale,
		isHotReloadable: isHotReloadable,
		defaultValue:    defaultValue,
		keys:            keys,
	}
	if isHotReloadable {
		hotReloadableConfig[keys[0]] = &configVar
	} else {
		nonHotReloadableConfig[keys[0]] = &configVar
	}
	for _, key := range keys {
		if IsSet(key) {
			isSet = true
			*ptr = GetDuration(key, defaultValue, timeScale)
			break
		}
	}
	if !isSet {
		*ptr = defaultValue * timeScale
	}
}

func RegisterStringConfigVariable(defaultValue string, ptr *string, isHotReloadable bool, keys ...string) {
	configVarLock.Lock()
	defer configVarLock.Unlock()
	var isSet bool
	configVar := ConfigVar{
		value:           ptr,
		isHotReloadable: isHotReloadable,
		defaultValue:    defaultValue,
		keys:            keys,
	}
	if isHotReloadable {
		hotReloadableConfig[keys[0]] = &configVar
	} else {
		nonHotReloadableConfig[keys[0]] = &configVar
	}
	for _, key := range keys {
		if IsSet(key) {
			isSet = true
			*ptr = GetString(key, defaultValue)
			break
		}
	}
	if !isSet {
		*ptr = defaultValue
	}
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
func GetDuration(key string, defaultValue time.Duration, timeScale time.Duration) (value time.Duration) {
	var envValue string
	envVal := GetEnv(TransformKey(key), "")
	if envVal != "" {
		envValue = cast.ToString(envVal)
		parseDuration, err := time.ParseDuration(envValue)
		if err == nil {
			return parseDuration
		} else {
			return cast.ToDuration(envVal) * timeScale
		}
	}

	if !viper.IsSet(key) {
		return defaultValue * timeScale
	} else {
		envValue = viper.GetString(key)
		parseDuration, err := time.ParseDuration(envValue)
		if err == nil {
			return parseDuration
		} else {
			_, err = strconv.ParseFloat(envValue, 64)
			if err == nil {
				return viper.GetDuration(key) * timeScale
			} else {
				return defaultValue * timeScale
			}
		}
	}
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
		panic(fmt.Errorf("fatal error, no required environment variable: %s", key))
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
	panic(fmt.Errorf("fatal error, no required environment variable: %s", key))
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

func GetNamespaceIdentifier() string {
	k8sNamespace := GetKubeNamespace()
	if k8sNamespace != "" {
		return k8sNamespace
	}
	return "none"
}

// returns value stored in KUBE_NAMESPACE env var
func GetKubeNamespace() string {
	return GetEnv("KUBE_NAMESPACE", "")
}

func GetInstanceID() string {
	return GetEnv("INSTANCE_ID", "1")
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

func GetArraySupportForCH() bool {
	return GetBool("Warehouse.clickhouse.enableArraySupport", false)
}

package config

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/utils/maputil"

	"golang.org/x/exp/slices"

	"github.com/fsnotify/fsnotify"
	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

func (c *Config) load() {
	c.hotReloadableConfig = make(map[string][]*configValue)
	c.envs = make(map[string]string)

	if err := godotenv.Load(); err != nil {
		fmt.Println("INFO: No .env file found.")
	}

	configPath := getEnv("CONFIG_PATH", "./config/config.yaml")

	v := viper.NewWithOptions(viper.EnvKeyReplacer(&envReplacer{c: c}))
	v.AutomaticEnv()
	bindLegacyEnv(v)

	v.SetConfigFile(configPath)
	err := v.ReadInConfig() // Find and read the config file
	// Don't panic if config.yaml is not found or error with parsing. Use the default config values instead
	if err != nil {
		fmt.Printf("[Config] :: Failed to parse config file from path %q, using default values: %v\n", configPath, err)
	}
	v.OnConfigChange(func(e fsnotify.Event) {
		c.onConfigChange()
	})
	v.WatchConfig()

	c.v = v
}

func (c *Config) onConfigChange() {
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("cannot update Config Variables: %v", r)
			fmt.Println(err)
		}
	}()
	c.vLock.RLock()
	defer c.vLock.RUnlock()
	c.hotReloadableConfigLock.RLock()
	defer c.hotReloadableConfigLock.RUnlock()
	c.checkAndHotReloadConfig(c.hotReloadableConfig)
}

func (c *Config) checkAndHotReloadConfig(configMap map[string][]*configValue) {
	for key, configValArr := range configMap {
		for _, configVal := range configValArr {
			value := configVal.value
			switch value := value.(type) {
			case *int:
				var _value int
				var isSet bool
				for _, key := range configVal.keys {
					if c.IsSet(key) {
						isSet = true
						_value = c.GetInt(key, configVal.defaultValue.(int))
						break
					}
				}
				if !isSet {
					_value = configVal.defaultValue.(int)
				}
				_value = _value * configVal.multiplier.(int)
				if _value != *value {
					fmt.Printf("The value of key:%s & variable:%p changed from %d to %d\n", key, configVal, *value, _value)
					*value = _value
				}
			case *int64:
				var _value int64
				var isSet bool
				for _, key := range configVal.keys {
					if c.IsSet(key) {
						isSet = true
						_value = c.GetInt64(key, configVal.defaultValue.(int64))
						break
					}
				}
				if !isSet {
					_value = configVal.defaultValue.(int64)
				}
				_value = _value * configVal.multiplier.(int64)
				if _value != *value {
					fmt.Printf("The value of key:%s & variable:%p changed from %d to %d\n", key, configVal, *value, _value)
					*value = _value
				}
			case *string:
				var _value string
				var isSet bool
				for _, key := range configVal.keys {
					if c.IsSet(key) {
						isSet = true
						_value = c.GetString(key, configVal.defaultValue.(string))
						break
					}
				}
				if !isSet {
					_value = configVal.defaultValue.(string)
				}
				if _value != *value {
					fmt.Printf("The value of key:%s & variable:%p changed from %v to %v\n", key, configVal, *value, _value)
					*value = _value
				}
			case *time.Duration:
				var _value time.Duration
				var isSet bool
				for _, key := range configVal.keys {
					if c.IsSet(key) {
						isSet = true
						_value = c.GetDuration(key, configVal.defaultValue.(int64), configVal.multiplier.(time.Duration))
						break
					}
				}
				if !isSet {
					_value = time.Duration(configVal.defaultValue.(int64)) * configVal.multiplier.(time.Duration)
				}
				if _value != *value {
					fmt.Printf("The value of key:%s & variable:%p changed from %v to %v\n", key, configVal, *value, _value)
					*value = _value
				}
			case *bool:
				var _value bool
				var isSet bool
				for _, key := range configVal.keys {
					if c.IsSet(key) {
						isSet = true
						_value = c.GetBool(key, configVal.defaultValue.(bool))
						break
					}
				}
				if !isSet {
					_value = configVal.defaultValue.(bool)
				}
				if _value != *value {
					fmt.Printf("The value of key:%s & variable:%p changed from %v to %v\n", key, configVal, *value, _value)
					*value = _value
				}
			case *float64:
				var _value float64
				var isSet bool
				for _, key := range configVal.keys {
					if c.IsSet(key) {
						isSet = true
						_value = c.GetFloat64(key, configVal.defaultValue.(float64))
						break
					}
				}
				if !isSet {
					_value = configVal.defaultValue.(float64)
				}
				_value = _value * configVal.multiplier.(float64)
				if _value != *value {
					fmt.Printf("The value of key:%s & variable:%p changed from %v to %v\n", key, configVal, *value, _value)
					*value = _value
				}
			case *[]string:
				var _value []string
				var isSet bool
				for _, key := range configVal.keys {
					if c.IsSet(key) {
						isSet = true
						_value = c.GetStringSlice(key, configVal.defaultValue.([]string))
						break
					}
				}
				if !isSet {
					_value = configVal.defaultValue.([]string)
				}
				if slices.Compare(_value, *value) != 0 {
					fmt.Printf("The value of key:%s & variable:%p changed from %v to %v\n", key, configVal, *value, _value)
					*value = _value
				}
			case *map[string]interface{}:
				var _value map[string]interface{}
				var isSet bool
				for _, key := range configVal.keys {
					if c.IsSet(key) {
						isSet = true
						_value = c.GetStringMap(key, configVal.defaultValue.(map[string]interface{}))
						break
					}
				}
				if !isSet {
					_value = configVal.defaultValue.(map[string]interface{})
				}

				if !maputil.Compare(_value, *value) {
					fmt.Printf("The value of key:%s & variable:%p changed from %v to %v\n", key, configVal, *value, _value)
					*value = _value
				}
			}
		}
	}
}

type configValue struct {
	value        interface{}
	multiplier   interface{}
	defaultValue interface{}
	keys         []string
}

func newConfigValue(value, multiplier, defaultValue interface{}, keys []string) *configValue {
	return &configValue{
		value:        value,
		multiplier:   multiplier,
		defaultValue: defaultValue,
		keys:         keys,
	}
}

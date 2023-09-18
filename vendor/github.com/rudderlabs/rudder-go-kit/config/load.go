package config

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/joho/godotenv"
	"github.com/spf13/viper"
	"golang.org/x/exp/slices"
)

func (c *Config) load() {
	c.hotReloadableConfig = make(map[string][]*configValue)
	c.envs = make(map[string]string)

	if err := godotenv.Load(); err != nil && !isTest() {
		fmt.Println("INFO: No .env file found.")
	}

	configPath := getEnv("CONFIG_PATH", "./config/config.yaml")

	v := viper.NewWithOptions(viper.EnvKeyReplacer(&envReplacer{c: c}))
	v.AutomaticEnv()
	bindLegacyEnv(v)

	v.SetConfigFile(configPath)
	err := v.ReadInConfig() // Find and read the config file
	// Don't panic if config.yaml is not found or error with parsing. Use the default config values instead
	if err != nil && !isTest() {
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
			case *int, *Reloadable[int]:
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
				swapHotReloadableConfig(key, "%d", configVal, value, _value, compare[int]())
			case *int64, *Reloadable[int64]:
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
				swapHotReloadableConfig(key, "%d", configVal, value, _value, compare[int64]())
			case *string, *Reloadable[string]:
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
				swapHotReloadableConfig(key, "%q", configVal, value, _value, compare[string]())
			case *time.Duration, *Reloadable[time.Duration]:
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
				swapHotReloadableConfig(key, "%d", configVal, value, _value, compare[time.Duration]())
			case *bool, *Reloadable[bool]:
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
				swapHotReloadableConfig(key, "%v", configVal, value, _value, compare[bool]())
			case *float64, *Reloadable[float64]:
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
				swapHotReloadableConfig(key, "%v", configVal, value, _value, compare[float64]())
			case *[]string, *Reloadable[[]string]:
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
				swapHotReloadableConfig(key, "%v", configVal, value, _value, func(a, b []string) bool {
					return slices.Compare(a, b) == 0
				})
			case *map[string]interface{}, *Reloadable[map[string]interface{}]:
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
				swapHotReloadableConfig(key, "%v", configVal, value, _value, func(a, b map[string]interface{}) bool {
					return mapDeepEqual(a, b)
				})
			}
		}
	}
}

func swapHotReloadableConfig[T configTypes](
	key, placeholder string, configVal *configValue, ptr any, newValue T,
	compare func(T, T) bool,
) {
	if value, ok := ptr.(*T); ok {
		if !compare(*value, newValue) {
			fmt.Printf("The value of key %q & variable %p changed from "+placeholder+" to "+placeholder+"\n",
				key, configVal, *value, newValue,
			)
			*value = newValue
		}
		return
	}
	reloadableValue, _ := configVal.value.(*Reloadable[T])
	if oldValue, swapped := reloadableValue.swapIfNotEqual(newValue, compare); swapped {
		fmt.Printf("The value of key %q & variable %p changed from "+placeholder+" to "+placeholder+"\n",
			key, configVal, oldValue, newValue,
		)
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

func mapDeepEqual[K comparable, V any](a, b map[K]V) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || !reflect.DeepEqual(v, w) {
			return false
		}
	}
	return true
}

func isTest() bool {
	for _, arg := range os.Args {
		if strings.HasPrefix(arg, "-test.") {
			return true
		}
	}
	return false
}

// Package config uses the exact same precedence order as Viper, where
// each item takes precedence over the item below it:
//
//   - explicit call to Set (case insensitive)
//   - flag (case insensitive)
//   - env (case sensitive - see notes below)
//   - config (case insensitive)
//   - key/value store (case insensitive)
//   - default (case insensitive)
//
// Environment variable resolution is performed based on the following rules:
//   - If the key contains only uppercase characters, numbers and underscores, the environment variable is looked up in its entirety, e.g. SOME_VARIABLE -> SOME_VARIABLE
//   - In all other cases, the environment variable is transformed before being looked up as following:
//     1. camelCase is converted to snake_case, e.g. someVariable -> some_variable
//     2. dots (.) are replaced with underscores (_), e.g. some.variable -> some_variable
//     3. the resulting string is uppercased and prefixed with RSERVER_, e.g. some_variable -> RSERVER_SOME_VARIABLE
package config

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"
)

// regular expression matching lowercase letter followed by an uppercase letter
var camelCaseMatch = regexp.MustCompile("([a-z0-9])([A-Z])")

// regular expression matching uppercase letters contained in environment variable names
var upperCaseMatch = regexp.MustCompile("^[A-Z0-9_]+$")

// default, singleton config instance
var defaultConfig *Config

func init() {
	defaultConfig = New()
}

// Reset resets configuration
func Reset() {
	defaultConfig = New()
}

// New creates a new config instance
func New() *Config {
	c := &Config{}
	c.load()
	return c
}

// Config is the entry point for accessing configuration
type Config struct {
	vLock                   sync.RWMutex // protects reading and writing to the config (viper is not thread-safe)
	v                       *viper.Viper
	hotReloadableConfigLock sync.RWMutex // protects map holding hot reloadable config keys
	hotReloadableConfig     map[string][]*configValue
	envsLock                sync.RWMutex // protects the envs map below
	envs                    map[string]string
}

// GetBool gets bool value from config
func GetBool(key string, defaultValue bool) (value bool) {
	return defaultConfig.GetBool(key, defaultValue)
}

// GetBool gets bool value from config
func (c *Config) GetBool(key string, defaultValue bool) (value bool) {
	c.vLock.RLock()
	defer c.vLock.RUnlock()
	if !c.IsSet(key) {
		return defaultValue
	}
	return c.v.GetBool(key)
}

// GetInt gets int value from config
func GetInt(key string, defaultValue int) (value int) {
	return defaultConfig.GetInt(key, defaultValue)
}

// GetInt gets int value from config
func (c *Config) GetInt(key string, defaultValue int) (value int) {
	c.vLock.RLock()
	defer c.vLock.RUnlock()
	if !c.IsSet(key) {
		return defaultValue
	}
	return c.v.GetInt(key)
}

// MustGetInt gets int value from config or panics if the config doesn't exist
func MustGetInt(key string) (value int) {
	return defaultConfig.MustGetInt(key)
}

// MustGetInt gets int value from config or panics if the config doesn't exist
func (c *Config) MustGetInt(key string) (value int) {
	c.vLock.RLock()
	defer c.vLock.RUnlock()
	if !c.IsSet(key) {
		panic(fmt.Errorf("config key %s not found", key))
	}
	return c.v.GetInt(key)
}

// GetInt64 gets int64 value from config
func GetInt64(key string, defaultValue int64) (value int64) {
	return defaultConfig.GetInt64(key, defaultValue)
}

// GetInt64 gets int64 value from config
func (c *Config) GetInt64(key string, defaultValue int64) (value int64) {
	c.vLock.RLock()
	defer c.vLock.RUnlock()
	if !c.IsSet(key) {
		return defaultValue
	}
	return c.v.GetInt64(key)
}

// GetFloat64 gets float64 value from config
func GetFloat64(key string, defaultValue float64) (value float64) {
	return defaultConfig.GetFloat64(key, defaultValue)
}

// GetFloat64 gets float64 value from config
func (c *Config) GetFloat64(key string, defaultValue float64) (value float64) {
	c.vLock.RLock()
	defer c.vLock.RUnlock()
	if !c.IsSet(key) {
		return defaultValue
	}
	return c.v.GetFloat64(key)
}

// GetString gets string value from config
func GetString(key, defaultValue string) (value string) {
	return defaultConfig.GetString(key, defaultValue)
}

// GetString gets string value from config
func (c *Config) GetString(key, defaultValue string) (value string) {
	c.vLock.RLock()
	defer c.vLock.RUnlock()
	if !c.IsSet(key) {
		return defaultValue
	}
	return c.v.GetString(key)
}

// MustGetString gets string value from config or panics if the config doesn't exist
func MustGetString(key string) (value string) {
	return defaultConfig.MustGetString(key)
}

// MustGetString gets string value from config or panics if the config doesn't exist
func (c *Config) MustGetString(key string) (value string) {
	c.vLock.RLock()
	defer c.vLock.RUnlock()
	if !c.IsSet(key) {
		panic(fmt.Errorf("config key %s not found", key))
	}
	return c.v.GetString(key)
}

// GetStringSlice gets string slice value from config
func (c *Config) GetStringSlice(key string, defaultValue []string) (value []string) {
	c.vLock.RLock()
	defer c.vLock.RUnlock()
	if !c.IsSet(key) {
		return defaultValue
	}
	return c.v.GetStringSlice(key)
}

// GetDuration gets duration value from config
func GetDuration(key string, defaultValueInTimescaleUnits int64, timeScale time.Duration) (value time.Duration) {
	return defaultConfig.GetDuration(key, defaultValueInTimescaleUnits, timeScale)
}

// GetDuration gets duration value from config
func (c *Config) GetDuration(key string, defaultValueInTimescaleUnits int64, timeScale time.Duration) (value time.Duration) {
	c.vLock.RLock()
	defer c.vLock.RUnlock()
	if !c.IsSet(key) {
		return time.Duration(defaultValueInTimescaleUnits) * timeScale
	} else {
		v := c.v.GetString(key)
		parseDuration, err := time.ParseDuration(v)
		if err == nil {
			return parseDuration
		} else {
			_, err = strconv.ParseFloat(v, 64)
			if err == nil {
				return c.v.GetDuration(key) * timeScale
			} else {
				return time.Duration(defaultValueInTimescaleUnits) * timeScale
			}
		}
	}
}

// IsSet checks if config is set for a key
func IsSet(key string) bool {
	return defaultConfig.IsSet(key)
}

// IsSet checks if config is set for a key
func (c *Config) IsSet(key string) bool {
	c.vLock.RLock()
	defer c.vLock.RUnlock()
	c.bindEnv(key)
	return c.v.IsSet(key)
}

// Override Config by application or command line

// Set override existing config
func Set(key string, value interface{}) {
	defaultConfig.Set(key, value)
}

// Set override existing config
func (c *Config) Set(key string, value interface{}) {
	c.vLock.Lock()
	c.v.Set(key, value)
	c.vLock.Unlock()
	c.onConfigChange()
}

// bindEnv handles rudder server's unique snake case replacement by registering
// the environment variables to viper, that would otherwise be ignored.
// Viper uppercases keys before sending them to its EnvKeyReplacer, thus
// the replacer cannot detect camelCase keys.
func (c *Config) bindEnv(key string) {
	envVar := key
	if !upperCaseMatch.MatchString(key) {
		envVar = ConfigKeyToEnv(key)
	}
	// bind once
	c.envsLock.RLock()
	if _, ok := c.envs[key]; !ok {
		c.envsLock.RUnlock()
		c.envsLock.Lock() // don't really care about race here, setting the same value
		c.envs[strings.ToUpper(key)] = envVar
		c.envsLock.Unlock()
	} else {
		c.envsLock.RUnlock()
	}
}

type envReplacer struct {
	c *Config
}

func (r *envReplacer) Replace(s string) string {
	r.c.envsLock.RLock()
	defer r.c.envsLock.RUnlock()
	if v, ok := r.c.envs[s]; ok {
		return v
	}
	return s // bound environment variables
}

// Fallback environment variables supported (historically) by rudder-server
func bindLegacyEnv(v *viper.Viper) {
	_ = v.BindEnv("DB.host", "JOBS_DB_HOST")
	_ = v.BindEnv("DB.user", "JOBS_DB_USER")
	_ = v.BindEnv("DB.name", "JOBS_DB_DB_NAME")
	_ = v.BindEnv("DB.port", "JOBS_DB_PORT")
	_ = v.BindEnv("DB.password", "JOBS_DB_PASSWORD")
	_ = v.BindEnv("DB.sslMode", "JOBS_DB_SSL_MODE")
	_ = v.BindEnv("SharedDB.dsn", "SHARED_DB_DSN")
}

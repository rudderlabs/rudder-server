package config

import "time"

// RegisterIntConfigVariable registers int config variable
func RegisterIntConfigVariable(defaultValue int, ptr *int, isHotReloadable bool, valueScale int, keys ...string) {
	c.RegisterIntConfigVariable(defaultValue, ptr, isHotReloadable, valueScale, keys...)
}

// RegisterIntConfigVariable registers int config variable
func (c *Config) RegisterIntConfigVariable(defaultValue int, ptr *int, isHotReloadable bool, valueScale int, keys ...string) {
	c.vLock.RLock()
	defer c.vLock.RUnlock()
	c.mapLock.Lock()
	defer c.mapLock.Unlock()
	configVar := configValue{
		value:           ptr,
		multiplier:      valueScale,
		isHotReloadable: isHotReloadable,
		defaultValue:    defaultValue,
		keys:            keys,
	}

	c.appendVarToConfigMaps(isHotReloadable, keys[0], &configVar)

	for _, key := range keys {
		if c.isSet(key) {
			*ptr = c.GetInt(key, defaultValue) * valueScale
			return
		}
	}
	*ptr = defaultValue * valueScale
}

// RegisterBoolConfigVariable registers bool config variable
func RegisterBoolConfigVariable(defaultValue bool, ptr *bool, isHotReloadable bool, keys ...string) {
	c.RegisterBoolConfigVariable(defaultValue, ptr, isHotReloadable, keys...)
}

// RegisterBoolConfigVariable registers bool config variable
func (c *Config) RegisterBoolConfigVariable(defaultValue bool, ptr *bool, isHotReloadable bool, keys ...string) {
	c.vLock.RLock()
	defer c.vLock.RUnlock()
	c.mapLock.Lock()
	defer c.mapLock.Unlock()
	configVar := configValue{
		value:           ptr,
		isHotReloadable: isHotReloadable,
		defaultValue:    defaultValue,
		keys:            keys,
	}

	c.appendVarToConfigMaps(isHotReloadable, keys[0], &configVar)

	for _, key := range keys {
		c.bindEnv(key)
		if c.isSet(key) {
			*ptr = c.GetBool(key, defaultValue)
			return
		}
	}
	*ptr = defaultValue
}

// RegisterFloat64ConfigVariable registers float64 config variable
func RegisterFloat64ConfigVariable(defaultValue float64, ptr *float64, isHotReloadable bool, keys ...string) {
	c.RegisterFloat64ConfigVariable(defaultValue, ptr, isHotReloadable, keys...)
}

// RegisterFloat64ConfigVariable registers float64 config variable
func (c *Config) RegisterFloat64ConfigVariable(defaultValue float64, ptr *float64, isHotReloadable bool, keys ...string) {
	c.vLock.RLock()
	defer c.vLock.RUnlock()
	c.mapLock.Lock()
	defer c.mapLock.Unlock()
	configVar := configValue{
		value:           ptr,
		multiplier:      1.0,
		isHotReloadable: isHotReloadable,
		defaultValue:    defaultValue,
		keys:            keys,
	}

	c.appendVarToConfigMaps(isHotReloadable, keys[0], &configVar)

	for _, key := range keys {
		c.bindEnv(key)
		if c.isSet(key) {
			*ptr = c.GetFloat64(key, defaultValue)
			return
		}
	}
	*ptr = defaultValue
}

// RegisterInt64ConfigVariable registers int64 config variable
func RegisterInt64ConfigVariable(defaultValue int64, ptr *int64, isHotReloadable bool, valueScale int64, keys ...string) {
	c.RegisterInt64ConfigVariable(defaultValue, ptr, isHotReloadable, valueScale, keys...)
}

// RegisterInt64ConfigVariable registers int64 config variable
func (c *Config) RegisterInt64ConfigVariable(defaultValue int64, ptr *int64, isHotReloadable bool, valueScale int64, keys ...string) {
	c.vLock.RLock()
	defer c.vLock.RUnlock()
	c.mapLock.Lock()
	defer c.mapLock.Unlock()
	configVar := configValue{
		value:           ptr,
		multiplier:      valueScale,
		isHotReloadable: isHotReloadable,
		defaultValue:    defaultValue,
		keys:            keys,
	}

	c.appendVarToConfigMaps(isHotReloadable, keys[0], &configVar)

	for _, key := range keys {
		c.bindEnv(key)
		if c.isSet(key) {
			*ptr = c.GetInt64(key, defaultValue) * valueScale
			return
		}
	}
	*ptr = defaultValue * valueScale
}

// RegisterDurationConfigVariable registers duration config variable
func RegisterDurationConfigVariable(defaultValueInTimescaleUnits int64, ptr *time.Duration, isHotReloadable bool, timeScale time.Duration, keys ...string) {
	c.RegisterDurationConfigVariable(defaultValueInTimescaleUnits, ptr, isHotReloadable, timeScale, keys...)
}

// RegisterDurationConfigVariable registers duration config variable
func (c *Config) RegisterDurationConfigVariable(defaultValueInTimescaleUnits int64, ptr *time.Duration, isHotReloadable bool, timeScale time.Duration, keys ...string) {
	c.vLock.RLock()
	defer c.vLock.RUnlock()
	c.mapLock.Lock()
	defer c.mapLock.Unlock()
	configVar := configValue{
		value:           ptr,
		multiplier:      timeScale,
		isHotReloadable: isHotReloadable,
		defaultValue:    defaultValueInTimescaleUnits,
		keys:            keys,
	}

	c.appendVarToConfigMaps(isHotReloadable, keys[0], &configVar)

	for _, key := range keys {
		if c.isSet(key) {
			*ptr = c.GetDuration(key, defaultValueInTimescaleUnits, timeScale)
			return
		}
	}
	*ptr = time.Duration(defaultValueInTimescaleUnits) * timeScale
}

// RegisterStringConfigVariable registers string config variable
func RegisterStringConfigVariable(defaultValue string, ptr *string, isHotReloadable bool, keys ...string) {
	c.RegisterStringConfigVariable(defaultValue, ptr, isHotReloadable, keys...)
}

// RegisterStringConfigVariable registers string config variable
func (c *Config) RegisterStringConfigVariable(defaultValue string, ptr *string, isHotReloadable bool, keys ...string) {
	c.vLock.RLock()
	defer c.vLock.RUnlock()
	c.mapLock.Lock()
	defer c.mapLock.Unlock()
	configVar := configValue{
		value:           ptr,
		isHotReloadable: isHotReloadable,
		defaultValue:    defaultValue,
		keys:            keys,
	}

	c.appendVarToConfigMaps(isHotReloadable, keys[0], &configVar)

	for _, key := range keys {
		if c.isSet(key) {
			*ptr = c.GetString(key, defaultValue)
			return
		}
	}
	*ptr = defaultValue
}

// RegisterStringSliceConfigVariable registers string slice config variable
func RegisterStringSliceConfigVariable(defaultValue []string, ptr *[]string, isHotReloadable bool, keys ...string) {
	c.RegisterStringSliceConfigVariable(defaultValue, ptr, isHotReloadable, keys...)
}

// RegisterStringSliceConfigVariable registers string slice config variable
func (c *Config) RegisterStringSliceConfigVariable(defaultValue []string, ptr *[]string, isHotReloadable bool, keys ...string) {
	c.vLock.RLock()
	defer c.vLock.RUnlock()
	c.mapLock.Lock()
	defer c.mapLock.Unlock()
	configVar := configValue{
		value:           ptr,
		isHotReloadable: isHotReloadable,
		defaultValue:    defaultValue,
		keys:            keys,
	}

	c.appendVarToConfigMaps(isHotReloadable, keys[0], &configVar)

	for _, key := range keys {
		if c.isSet(key) {
			*ptr = c.GetStringSlice(key, defaultValue)
			return
		}
	}
	*ptr = defaultValue
}

func (c *Config) appendVarToConfigMaps(isHotReloadable bool, key string, configVar *configValue) {
	if isHotReloadable {
		if _, ok := c.hotReloadableConfig[key]; !ok {
			c.hotReloadableConfig[key] = make([]*configValue, 0)
		}
		c.hotReloadableConfig[key] = append(c.hotReloadableConfig[key], configVar)
	} else {
		if _, ok := c.nonHotReloadableConfig[key]; !ok {
			c.nonHotReloadableConfig[key] = make([]*configValue, 0)
		}
		c.nonHotReloadableConfig[key] = append(c.nonHotReloadableConfig[key], configVar)
	}
}

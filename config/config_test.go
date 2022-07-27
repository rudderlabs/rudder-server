package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStatic_checkAndHotReloadConfig(t *testing.T) {
	configMap := make(map[string][]*ConfigVar)

	var var1 string
	var var2 string
	configVar1 := newConfigVar(&var1, 1, "var1", true, []string{"KEY_VAR"})
	configVar2 := newConfigVar(&var2, 1, "var2", true, []string{"KEY_VAR"})

	configMap["KEY_VAR"] = []*ConfigVar{configVar1, configVar2}
	t.Setenv("RSERVER_KEY_VAR", "value_changed")

	checkAndHotReloadConfig(configMap)

	varptr1 := getConfigValue(configVar1).(*string)
	varptr2 := getConfigValue(configVar2).(*string)
	require.Equal(t, *varptr1, "value_changed")
	require.Equal(t, *varptr2, "value_changed")
}

func TestStatic_RegisterAndDeRegister(t *testing.T) {
	hotReloadableConfig = make(map[string][]*ConfigVar)
	nonHotReloadableConfig = make(map[string][]*ConfigVar)

	var timeout1 time.Duration
	RegisterDurationConfigVariable(10, &timeout1, true, time.Second, []string{"KEY_VAR"}...)

	require.Equal(t, len(hotReloadableConfig), 1)
	require.Equal(t, len(nonHotReloadableConfig), 0)

	var timeout2 time.Duration
	RegisterDurationConfigVariable(10, &timeout2, false, time.Second, []string{"KEY_VAR"}...)

	require.Equal(t, len(hotReloadableConfig), 1)
	require.Equal(t, len(nonHotReloadableConfig), 1)

	DeRegisterConfigVariableWithPtr(&timeout2)

	require.Equal(t, len(hotReloadableConfig), 1)
	require.Equal(t, len(nonHotReloadableConfig), 0)

	DeRegisterConfigVariableWithPtr(&timeout1)

	require.Equal(t, len(hotReloadableConfig), 0)
	require.Equal(t, len(nonHotReloadableConfig), 0)
}

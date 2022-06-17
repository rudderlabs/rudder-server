package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatic_checkAndHotReloadConfig(t *testing.T) {
	configMap := make(map[string][]*ConfigVar)

	var var1 string
	var var2 string
	configVar1 := newConfigVar(&var1, 1, "var1", true, []string{"KEY_VAR"})
	configVar2 := newConfigVar(&var2, 1, "var2", true, []string{"KEY_VAR"})

	configMap["KEY_VAR"] = []*ConfigVar{configVar1, configVar2}
	require.NoError(t, os.Setenv("RSERVER_KEY_VAR", "value_changed"))

	checkAndHotReloadConfig(configMap)

	varptr1 := getConfigValue(configVar1).(*string)
	varptr2 := getConfigValue(configVar2).(*string)
	require.Equal(t, *varptr1, "value_changed")
	require.Equal(t, *varptr2, "value_changed")
}

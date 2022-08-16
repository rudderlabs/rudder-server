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

func TestGetFirst(t *testing.T) {
	Load()

	t.Run("get first duration", func(t *testing.T) {
		d := GetFirstDuration(1, time.Second, "oneD", "twoD")
		require.Equal(t, time.Second, d)

		t.Setenv("RSERVER_TWO_D", "2s")
		d = GetFirstDuration(1, time.Second, "oneD", "twoD")
		require.Equal(t, 2*time.Second, d)

		t.Setenv("RSERVER_ONE_D", "3s")
		d = GetFirstDuration(1, time.Second, "oneD", "twoD")
		require.Equal(t, 3*time.Second, d)
	})

	t.Run("get first string", func(t *testing.T) {
		s := GetFirstString("1s", "oneS", "twoS")
		require.Equal(t, "1s", s)

		t.Setenv("RSERVER_TWO_S", "2s")
		s = GetFirstString("1s", "oneS", "twoS")
		require.Equal(t, "2s", s)

		t.Setenv("RSERVER_ONE_S", "3s")
		s = GetFirstString("1s", "oneS", "twoS")
		require.Equal(t, "3s", s)
	})

	t.Run("get first int", func(t *testing.T) {
		i := GetFirstInt(1, "oneI", "twoI")
		require.Equal(t, 1, i)

		t.Setenv("RSERVER_TWO_I", "2")
		i = GetFirstInt(1, "oneI", "twoI")
		require.Equal(t, 2, i)

		t.Setenv("RSERVER_ONE_I", "3")
		i = GetFirstInt(1, "oneI", "twoI")
		require.Equal(t, 3, i)
	})

	t.Run("get first float64", func(t *testing.T) {
		f := GetFirstFloat64(1, "oneF", "twoF")
		require.Equal(t, 1.0, f)

		t.Setenv("RSERVER_TWO_F", "2")
		f = GetFirstFloat64(1, "oneF", "twoF")
		require.Equal(t, 2.0, f)

		t.Setenv("RSERVER_ONE_F", "3")
		f = GetFirstFloat64(1, "oneF", "twoF")
		require.Equal(t, 3.0, f)
	})

	t.Run("get first bool", func(t *testing.T) {
		b := GetFirstBool(true, "oneB", "twoB")
		require.Equal(t, true, b)

		t.Setenv("RSERVER_TWO_B", "false")
		b = GetFirstBool(true, "oneB", "twoB")
		require.Equal(t, false, b)

		t.Setenv("RSERVER_ONE_B", "true")
		b = GetFirstBool(true, "oneF", "twoF")
		require.Equal(t, true, b)
	})

}

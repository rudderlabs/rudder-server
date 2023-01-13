package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_Getters_Existing_and_Default(t *testing.T) {
	tc := New()
	tc.Set("string", "string")
	require.Equal(t, "string", tc.GetString("string", "default"), "it should return the key value")
	require.Equal(t, "default", tc.GetString("other", "default"), "it should return the default value")

	tc.Set("bool", false)
	require.Equal(t, false, tc.GetBool("bool", true), "it should return the key value")
	require.Equal(t, true, tc.GetBool("other", true), "it should return the default value")

	tc.Set("int", 0)
	require.Equal(t, 0, tc.GetInt("int", 1), "it should return the key value")
	require.Equal(t, 1, tc.GetInt("other", 1), "it should return the default value")
	require.EqualValues(t, 0, tc.GetInt64("int", 1), "it should return the key value")
	require.EqualValues(t, 1, tc.GetInt64("other", 1), "it should return the default value")

	tc.Set("float", 0.0)
	require.EqualValues(t, 0, tc.GetFloat64("float", 1), "it should return the key value")
	require.EqualValues(t, 1, tc.GetFloat64("other", 1), "it should return the default value")

	tc.Set("stringslice", []string{"string", "string"})
	require.Equal(t, []string{"string", "string"}, tc.GetStringSlice("stringslice", []string{"default"}), "it should return the key value")
	require.Equal(t, []string{"default"}, tc.GetStringSlice("other", []string{"default"}), "it should return the default value")

	tc.Set("duration", "2ms")
	require.Equal(t, 2*time.Millisecond, tc.GetDuration("duration", 1, time.Second), "it should return the key value")
	require.Equal(t, time.Second, tc.GetDuration("other", 1, time.Second), "it should return the default value")

	tc.Set("duration", "2")
	require.Equal(t, 2*time.Second, tc.GetDuration("duration", 1, time.Second), "it should return the key value")
	require.Equal(t, time.Second, tc.GetDuration("other", 1, time.Second), "it should return the default value")

	tc.Set("stringmap", map[string]interface{}{"string": "any"})
	require.Equal(t, map[string]interface{}{"string": "any"}, tc.GetStringMap("stringmap", map[string]interface{}{"default": "value"}), "it should return the key value")
	require.Equal(t, map[string]interface{}{"default": "value"}, tc.GetStringMap("other", map[string]interface{}{"default": "value"}), "it should return the default value")
}

func Test_MustGet(t *testing.T) {
	tc := New()
	tc.Set("string", "string")
	require.Equal(t, "string", tc.MustGetString("string"), "it should return the key value")
	require.Panics(t, func() { tc.MustGetString("other") })

	tc.Set("int", 0)
	require.Equal(t, 0, tc.MustGetInt("int"), "it should return the key value")
	require.Panics(t, func() { tc.MustGetInt("other") })
}

func Test_Register_Existing_and_Default(t *testing.T) {
	tc := New()
	tc.Set("string", "string")
	var stringValue string
	var otherStringValue string
	tc.RegisterStringConfigVariable("default", &stringValue, false, "string")
	require.Equal(t, "string", stringValue, "it should return the key value")
	tc.RegisterStringConfigVariable("default", &otherStringValue, false, "other")
	require.Equal(t, "default", otherStringValue, "it should return the default value")

	tc.Set("bool", false)
	var boolValue bool
	var otherBoolValue bool
	tc.RegisterBoolConfigVariable(true, &boolValue, false, "bool")
	require.Equal(t, false, boolValue, "it should return the key value")
	tc.RegisterBoolConfigVariable(true, &otherBoolValue, false, "other")
	require.Equal(t, true, otherBoolValue, "it should return the default value")

	tc.Set("int", 0)
	var intValue int
	var otherIntValue int
	var int64Value int64
	var otherInt64Value int64
	tc.RegisterIntConfigVariable(1, &intValue, false, 1, "int")
	require.Equal(t, 0, intValue, "it should return the key value")
	tc.RegisterIntConfigVariable(1, &otherIntValue, false, 1, "other")
	require.Equal(t, 1, otherIntValue, "it should return the default value")
	tc.RegisterInt64ConfigVariable(1, &int64Value, false, 1, "int")
	require.EqualValues(t, 0, int64Value, "it should return the key value")
	tc.RegisterInt64ConfigVariable(1, &otherInt64Value, false, 1, "other")
	require.EqualValues(t, 1, otherInt64Value, "it should return the default value")

	tc.Set("float", 0.0)
	var floatValue float64
	var otherFloatValue float64
	tc.RegisterFloat64ConfigVariable(1, &floatValue, false, "float")
	require.EqualValues(t, 0, floatValue, "it should return the key value")
	tc.RegisterFloat64ConfigVariable(1, &otherFloatValue, false, "other")
	require.EqualValues(t, 1, otherFloatValue, "it should return the default value")

	tc.Set("stringslice", []string{"string", "string"})
	var stringSliceValue []string
	var otherStringSliceValue []string
	tc.RegisterStringSliceConfigVariable([]string{"default"}, &stringSliceValue, false, "stringslice")
	require.Equal(t, []string{"string", "string"}, stringSliceValue, "it should return the key value")
	tc.RegisterStringSliceConfigVariable([]string{"default"}, &otherStringSliceValue, false, "other")
	require.Equal(t, []string{"default"}, otherStringSliceValue, "it should return the default value")

	tc.Set("duration", "2ms")
	var durationValue time.Duration
	var otherDurationValue time.Duration
	tc.RegisterDurationConfigVariable(1, &durationValue, false, time.Second, "duration")
	require.Equal(t, 2*time.Millisecond, durationValue, "it should return the key value")
	tc.RegisterDurationConfigVariable(1, &otherDurationValue, false, time.Second, "other")
	require.Equal(t, time.Second, otherDurationValue, "it should return the default value")

	tc.Set("stringmap", map[string]interface{}{"string": "any"})
	var stringMapValue map[string]interface{}
	var otherStringMapValue map[string]interface{}
	tc.RegisterStringMapConfigVariable(map[string]interface{}{"default": "value"}, &stringMapValue, false, "stringmap")
	require.Equal(t, map[string]interface{}{"string": "any"}, stringMapValue, "it should return the key value")
	tc.RegisterStringMapConfigVariable(map[string]interface{}{"default": "value"}, &otherStringMapValue, false, "other")
	require.Equal(t, map[string]interface{}{"default": "value"}, otherStringMapValue, "it should return the default value")
}

func TestStatic_checkAndHotReloadConfig(t *testing.T) {
	configMap := make(map[string][]*configValue)

	var var1 string
	var var2 string
	configVar1 := newConfigValue(&var1, 1, "var1", []string{"keyVar"})
	configVar2 := newConfigValue(&var2, 1, "var2", []string{"keyVar"})

	configMap["keyVar"] = []*configValue{configVar1, configVar2}
	t.Setenv("RSERVER_KEY_VAR", "value_changed")

	Default.checkAndHotReloadConfig(configMap)

	varptr1 := configVar1.value.(*string)
	varptr2 := configVar2.value.(*string)
	require.Equal(t, *varptr1, "value_changed")
	require.Equal(t, *varptr2, "value_changed")
}

func TestCheckAndHotReloadConfig(t *testing.T) {
	var (
		stringValue            string
		stringConfigValue      = newConfigValue(&stringValue, nil, "default", []string{"string"})
		boolValue              bool
		boolConfigValue        = newConfigValue(&boolValue, nil, false, []string{"bool"})
		intValue               int
		intConfigValue         = newConfigValue(&intValue, 1, 0, []string{"int"})
		int64Value             int64
		int64ConfigValue       = newConfigValue(&int64Value, int64(1), int64(0), []string{"int64"})
		float64Value           float64
		float64ConfigValue     = newConfigValue(&float64Value, 1.0, 0.0, []string{"float64"})
		stringSliceValue       []string
		stringSliceConfigValue = newConfigValue(&stringSliceValue, nil, []string{"default"}, []string{"stringslice"})
		durationValue          time.Duration
		durationConfigValue    = newConfigValue(&durationValue, time.Second, int64(1), []string{"duration"})
		stringMapValue         map[string]interface{}
		stringMapConfigValue   = newConfigValue(&stringMapValue, nil, map[string]interface{}{"default": "value"}, []string{"stringmap"})
	)

	t.Run("with envs", func(t *testing.T) {
		t.Setenv("RSERVER_INT", "1")
		t.Setenv("RSERVER_INT64", "1")
		t.Setenv("RSERVER_STRING", "string")
		t.Setenv("RSERVER_DURATION", "2s")
		t.Setenv("RSERVER_BOOL", "true")
		t.Setenv("RSERVER_FLOAT64", "1.0")
		t.Setenv("RSERVER_STRINGSLICE", "string string")
		t.Setenv("RSERVER_STRINGMAP", "{\"string\":\"any\"}")

		Default.checkAndHotReloadConfig(map[string][]*configValue{
			"string":      {stringConfigValue},
			"bool":        {boolConfigValue},
			"int":         {intConfigValue},
			"int64":       {int64ConfigValue},
			"float64":     {float64ConfigValue},
			"stringslice": {stringSliceConfigValue},
			"duration":    {durationConfigValue},
			"stringmap":   {stringMapConfigValue},
		})

		require.Equal(t, *stringConfigValue.value.(*string), "string")
		require.Equal(t, *boolConfigValue.value.(*bool), true)
		require.Equal(t, *intConfigValue.value.(*int), 1)
		require.Equal(t, *int64ConfigValue.value.(*int64), int64(1))
		require.Equal(t, *float64ConfigValue.value.(*float64), 1.0)
		require.Equal(t, *durationConfigValue.value.(*time.Duration), 2*time.Second)
		require.Equal(t, *stringSliceConfigValue.value.(*[]string), []string{"string", "string"})
		require.Equal(t, *stringMapConfigValue.value.(*map[string]any), map[string]any{"string": "any"})
	})

	t.Run("without envs", func(t *testing.T) {
		Default.checkAndHotReloadConfig(map[string][]*configValue{
			"string":      {stringConfigValue},
			"bool":        {boolConfigValue},
			"int":         {intConfigValue},
			"int64":       {int64ConfigValue},
			"float64":     {float64ConfigValue},
			"stringslice": {stringSliceConfigValue},
			"duration":    {durationConfigValue},
			"stringmap":   {stringMapConfigValue},
		})

		require.Equal(t, *stringConfigValue.value.(*string), "default")
		require.Equal(t, *boolConfigValue.value.(*bool), false)
		require.Equal(t, *intConfigValue.value.(*int), 0)
		require.Equal(t, *int64ConfigValue.value.(*int64), int64(0))
		require.Equal(t, *float64ConfigValue.value.(*float64), 0.0)
		require.Equal(t, *durationConfigValue.value.(*time.Duration), 1*time.Second)
		require.Equal(t, *stringSliceConfigValue.value.(*[]string), []string{"default"})
		require.Equal(t, *stringMapConfigValue.value.(*map[string]any), map[string]any{"default": "value"})
	})
}

func TestConfigKeyToEnv(t *testing.T) {
	expected := "RSERVER_KEY_VAR1_VAR2"
	require.Equal(t, expected, ConfigKeyToEnv("Key.Var1.Var2"))
	require.Equal(t, expected, ConfigKeyToEnv("key.var1.var2"))
	require.Equal(t, expected, ConfigKeyToEnv("KeyVar1Var2"))
	require.Equal(t, expected, ConfigKeyToEnv("RSERVER_KEY_VAR1_VAR2"))
	require.Equal(t, "KEY_VAR1_VAR2", ConfigKeyToEnv("KEY_VAR1_VAR2"))
}

func TestGetEnvThroughViper(t *testing.T) {
	expectedValue := "VALUE"

	t.Run("detects dots", func(t *testing.T) {
		t.Setenv("RSERVER_KEY_VAR1_VAR2", expectedValue)
		tc := New()
		require.Equal(t, expectedValue, tc.GetString("Key.Var1.Var2", ""))
	})

	t.Run("detects camelcase", func(t *testing.T) {
		t.Setenv("RSERVER_KEY_VAR1_VAR2", expectedValue)
		tc := New()
		require.Equal(t, expectedValue, tc.GetString("KeyVar1Var2", ""))
	})

	t.Run("detects dots with camelcase", func(t *testing.T) {
		t.Setenv("RSERVER_KEY_VAR1_VAR_VAR", expectedValue)
		tc := New()
		require.Equal(t, expectedValue, tc.GetString("Key.Var1VarVar", ""))
	})

	t.Run("detects uppercase env variables", func(t *testing.T) {
		t.Setenv("SOMEENVVARIABLE", expectedValue)
		tc := New()
		require.Equal(t, expectedValue, tc.GetString("SOMEENVVARIABLE", ""))

		t.Setenv("SOME_ENV_VARIABLE", expectedValue)
		require.Equal(t, expectedValue, tc.GetString("SOME_ENV_VARIABLE", ""))

		t.Setenv("SOME_ENV_VARIABLE12", expectedValue)
		require.Equal(t, expectedValue, tc.GetString("SOME_ENV_VARIABLE12", ""))
	})

	t.Run("doesn't use viper's default env var matcher (uppercase)", func(t *testing.T) {
		t.Setenv("KEYVAR1VARVAR", expectedValue)
		tc := New()
		require.Equal(t, "", tc.GetString("KeyVar1VarVar", ""))
	})

	t.Run("can retrieve legacy env", func(t *testing.T) {
		t.Setenv("JOBS_DB_HOST", expectedValue)
		tc := New()
		require.Equal(t, expectedValue, tc.GetString("DB.host", ""))
	})
}

func TestRegisterEnvThroughViper(t *testing.T) {
	expectedValue := "VALUE"

	t.Run("detects dots", func(t *testing.T) {
		t.Setenv("RSERVER_KEY_VAR1_VAR2", expectedValue)
		tc := New()
		var v string
		tc.RegisterStringConfigVariable("", &v, true, "Key.Var1.Var2")
		require.Equal(t, expectedValue, v)
	})

	t.Run("detects camelcase", func(t *testing.T) {
		t.Setenv("RSERVER_KEY_VAR_VAR", expectedValue)
		tc := New()
		var v string
		tc.RegisterStringConfigVariable("", &v, true, "KeyVarVar")
		require.Equal(t, expectedValue, v)
	})

	t.Run("detects dots with camelcase", func(t *testing.T) {
		t.Setenv("RSERVER_KEY_VAR1_VAR_VAR", expectedValue)
		tc := New()
		var v string
		tc.RegisterStringConfigVariable("", &v, true, "Key.Var1VarVar")
		require.Equal(t, expectedValue, v)
	})
}

func Test_Set_CaseInsensitive(t *testing.T) {
	tc := New()
	tc.Set("sTrIng.One", "string")
	require.Equal(t, "string", tc.GetString("String.one", "default"), "it should return the key value")
}

func Test_Misc(t *testing.T) {
	t.Setenv("KUBE_NAMESPACE", "value")
	require.Equal(t, "value", GetKubeNamespace())

	t.Setenv("KUBE_NAMESPACE", "")
	require.Equal(t, "none", GetNamespaceIdentifier())

	t.Setenv("WORKSPACE_TOKEN", "value1")
	t.Setenv("CONFIG_BACKEND_TOKEN", "value2")
	require.Equal(t, "value1", GetWorkspaceToken())

	t.Setenv("WORKSPACE_TOKEN", "")
	t.Setenv("CONFIG_BACKEND_TOKEN", "value2")
	require.Equal(t, "value2", GetWorkspaceToken())

	t.Setenv("RELEASE_NAME", "value")
	require.Equal(t, "value", GetReleaseName())

	t.Setenv("INSTANCE_ID", "allbirds-v0-rudderstack-gw-ha-0-85d66f748f-8w4td")
	require.Equal(t, "0", GetInstanceID())

	t.Setenv("INSTANCE_ID", "prousmtusmt-v0-rs-gw-0")
	require.Equal(t, "0", GetInstanceID())

	t.Setenv("INSTANCE_ID", "prousmtusmt-v0-rs")
	require.Equal(t, "", GetInstanceID())
}

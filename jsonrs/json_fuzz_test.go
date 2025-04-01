package jsonrs_test

import (
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	trand "github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/jsonrs"
)

func FuzzSonnet(f *testing.F) {
	fuzzScenario(jsonrs.SonnetLib, f)
}

func FuzzJsoniter(f *testing.F) {
	fuzzScenario(jsonrs.JsoniterLib, f)
}

type Scenario struct {
	StringValue string
	UintValue   uint64
	IntValue    int64
	FloatValue  float64
	BoolValue   bool
}

func fuzzScenario(name string, f *testing.F) {
	j := jsonrs.NewWithLibrary(name)

	seed := []Scenario{
		{}, // zero values
		{StringValue: "hello", UintValue: 1, IntValue: 1, FloatValue: 0.1, BoolValue: true},
		{StringValue: "foo", UintValue: math.MaxUint64, IntValue: math.MaxInt64, FloatValue: math.MaxFloat64},
		{StringValue: "bar", UintValue: math.MaxUint32, IntValue: math.MaxInt32, FloatValue: float64(math.MaxFloat32)},
		{StringValue: `{"key": "string containing json"}`, UintValue: math.MaxUint16, IntValue: math.MaxInt16, FloatValue: float64(math.SmallestNonzeroFloat64)},
		{StringValue: "!@#$%^", UintValue: math.MaxUint8, IntValue: math.MaxInt8, FloatValue: float64(math.SmallestNonzeroFloat64)},
		{StringValue: trand.String(10000), FloatValue: 3.4028235e+38},
		{FloatValue: -26413875975788446000},
		{FloatValue: 1.1e3},
		{FloatValue: 1.1e-2},
	}

	for _, s := range seed {
		f.Add(s.FloatValue, s.IntValue, s.UintValue, s.StringValue, s.BoolValue)
	}

	f.Fuzz(func(t *testing.T, float64Value float64, int64Value int64, uint64Value uint64, stringValue string, boolValue bool) {
		if math.IsInf(float64Value, 0) || math.IsNaN(float64Value) {
			float64Value = 0
		}
		verify(t, name, j, float64Value)          // float64
		verifyAsFloat32(t, name, j, float64Value) // float64 as float32
		verify(t, name, j, float32(float64Value)) // float32
		verify(t, name, j, any(float64Value))     // float64 as any

		verify(t, name, j, int64Value)        // int64
		verify(t, name, j, int32(int64Value)) // int32
		verify(t, name, j, int16(int64Value)) // int16
		verify(t, name, j, int8(int64Value))  // int8
		verify(t, name, j, int(int64Value))   // int
		verify(t, name, j, any(int64Value))   // int64 as any

		verify(t, name, j, uint64Value)         // uint64
		verify(t, name, j, uint32(uint64Value)) // uint32
		verify(t, name, j, uint16(uint64Value)) // uint16
		verify(t, name, j, uint8(uint64Value))  // uint8
		verify(t, name, j, uint(uint64Value))   // uint
		verify(t, name, j, any(uint64Value))    // uint64 as any

		verify(t, name, j, stringValue)      // string
		verify(t, name, j, any(stringValue)) // string as any

		verify(t, name, j, boolValue)      // bool
		verify(t, name, j, any(boolValue)) // bool as any

		s := Scenario{
			StringValue: stringValue,
			UintValue:   uint64Value,
			IntValue:    int64Value,
			FloatValue:  float64Value,
			BoolValue:   boolValue,
		}
		verify(t, name, j, s) // struct
		verify(t, name, j, any(s))

		m := map[string]any{
			"stringValue": stringValue,
			"uint":        uint64Value,
			"int":         int64Value,
			"float":       float64Value,
			"bool":        boolValue,
			"struct":      s,
		}

		verify(t, name, j, m) // map[string]any

		verify(t, name, j, any(m)) // map[string]any as any

		verify(t, name, j, []any{m, m})      // slice
		verify(t, name, j, any([]any{m, m})) // slice as any
	})
}

func verify[T any](t *testing.T, libName string, j jsonrs.JSON, value T) {
	std := jsonrs.NewWithLibrary(jsonrs.StdLib)
	valueType := reflect.TypeOf(value)

	jsonValue, err := std.Marshal(value)
	if err != nil {
		_, jerr := j.Marshal(value)
		require.Errorf(t, jerr, "%s shouldn't be able to marshal %s that std lib cannot marshal: %v, err: %v", valueType, value, err)
		return
	}
	require.NoErrorf(t, std.Unmarshal(jsonValue, &value), "std json library should be able to unmarshal %q to %s", string(jsonValue), valueType)

	// Marshal with library
	libJsonValue, err := j.Marshal(value)
	require.NoErrorf(t, err, "%s should be able to marshal %s: %v", libName, valueType, value)
	require.JSONEqf(t, string(jsonValue), string(libJsonValue), "unexpected marshal result for %v", value)

	// Unmarshal with library
	var libValue T
	err = j.Unmarshal(jsonValue, &libValue)
	require.NoErrorf(t, err, "%s should be able to unmarshal %q to %s (std unmarshalled to: %v)", libName, string(jsonValue), valueType, value)

	require.EqualValues(t, value, libValue, "unexpected unmarshal result for %v", value)
}

func verifyAsFloat32(t *testing.T, libName string, j jsonrs.JSON, value float64) {
	if math.IsInf(value, 0) || math.IsNaN(value) {
		return
	}
	std := jsonrs.NewWithLibrary(jsonrs.StdLib)
	jsonValue, err := std.Marshal(value)

	var f32Std float32
	err1 := std.Unmarshal(jsonValue, &f32Std)
	var f32Lib float32
	err2 := j.Unmarshal(jsonValue, &f32Lib)

	if err1 != nil {
		require.Errorf(t, err2, "%s shouldn't be able to unmarshal %f that std lib cannot unmarshal: %v, err: %v", libName, value, err1)
		return
	} else {
		require.NoErrorf(t, err2, "%s should be able to unmarshal %q to float32: %v", libName, jsonValue, err2)
	}

	require.NoError(t, err)
}

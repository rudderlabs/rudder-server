package source

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_CustomTime(t *testing.T) {
	bytes := []byte(`{"t": "12-24-2023 12:05:07"}`)

	type SomeStruct struct {
		T CustomTime `json:"t"`
	}

	t.Run("Unmarshal valid time format", func(t *testing.T) {
		var s SomeStruct
		require.NoError(t, json.Unmarshal(bytes, &s))
		require.Equal(t, "12-24-2023 12:05:07", s.T.Format(CustomTimeLayout))
	})

	t.Run("Unmarshal null time", func(t *testing.T) {
		var s SomeStruct
		bytes = []byte(`{"t": null}`)
		require.NoError(t, json.Unmarshal(bytes, &s))
		require.True(t, s.T.IsZero())
	})

	t.Run("Unmarshal empty payload", func(t *testing.T) {
		var s SomeStruct
		bytes = []byte(`{}`)
		require.NoError(t, json.Unmarshal(bytes, &s))
		require.True(t, s.T.IsZero())
	})

	t.Run("Unmarshal different time format", func(t *testing.T) {
		var s SomeStruct
		bytes = []byte(`{"t": "2023-12-24T12:05:07"}`)
		require.Error(t, json.Unmarshal(bytes, &s))
		require.True(t, s.T.IsZero())
	})

	t.Run("marshal valid time", func(t *testing.T) {
		timeStamp := time.Date(2023, 12, 24, 12, 5, 7, 0, time.UTC)
		s := SomeStruct{T: CustomTime{timeStamp}}
		b, err := json.Marshal(s)
		require.NoError(t, err)
		require.Equal(t, `{"t":"12-24-2023 12:05:07"}`, string(b))
	})

	t.Run("marshal zero time", func(t *testing.T) {
		s := SomeStruct{T: CustomTime{}}
		b, err := json.Marshal(s)
		require.NoError(t, err)
		require.Equal(t, `{"t":null}`, string(b))
	})
}

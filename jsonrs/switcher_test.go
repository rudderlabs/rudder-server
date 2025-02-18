package jsonrs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSwitcher(t *testing.T) {
	marshaller := StdLib
	unmarshaller := StdLib

	switcher := &switcher{
		marshaller:   func() string { return marshaller },
		unmarshaller: func() string { return unmarshaller },
		impls: map[string]JSON{
			StdLib:      &stdJSON{},
			JsoniterLib: &jsoniterJSON{},
			SonnetLib:   &sonnetJSON{},
		},
	}

	t.Run("marshal", func(t *testing.T) {
		v, err := switcher.Marshal("text")
		require.NoError(t, err)
		require.Equal(t, []byte(`"text"`), v)
	})

	t.Run("unmarshal", func(t *testing.T) {
		var text string
		err := switcher.Unmarshal([]byte(`"text"`), &text)
		require.NoError(t, err)
		require.Equal(t, "text", text)
	})

	t.Run("invalid config uses default", func(t *testing.T) {
		marshaller = "invalid"
		unmarshaller = "invalid"

		v, err := switcher.Marshal("text")
		require.NoError(t, err)
		require.Equal(t, []byte(`"text"`), v)

		var text string
		err = switcher.Unmarshal([]byte(`"text"`), &text)
		require.NoError(t, err)
	})
}

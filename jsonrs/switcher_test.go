package jsonrs

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSwitcher(t *testing.T) {
	marshaller := StdLib
	unmarshaller := StdLib

	switcher := &switcher{
		marshallerFn:   func() string { return marshaller },
		unmarshallerFn: func() string { return unmarshaller },
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

	t.Run("proper marshaller and unmarshaller", func(t *testing.T) {
		oneJSON := &mockJSON{}
		twoJSON := &mockJSON{}
		switcher.impls["one"] = oneJSON
		switcher.impls["two"] = twoJSON
		marshaller = "one"
		unmarshaller = "two"

		_, _ = switcher.Marshal("")
		require.Equal(t, 1, oneJSON.called)
		_, _ = switcher.MarshalIndent("", "", "")
		require.Equal(t, 2, oneJSON.called)
		_, _ = switcher.MarshalToString("")
		require.Equal(t, 3, oneJSON.called)
		_ = switcher.NewEncoder(nil)
		require.Equal(t, 4, oneJSON.called)

		_ = switcher.Unmarshal([]byte(`""`), nil)
		require.Equal(t, 1, twoJSON.called)
		_ = switcher.NewDecoder(nil)
		require.Equal(t, 2, twoJSON.called)
	})
}

type mockJSON struct {
	called int
}

func (m *mockJSON) Marshal(v any) ([]byte, error) {
	m.called++
	return nil, nil
}

func (m *mockJSON) MarshalIndent(v any, prefix, indent string) ([]byte, error) {
	m.called++
	return nil, nil
}

func (m *mockJSON) Unmarshal(data []byte, v any) error {
	m.called++
	return nil
}

func (m *mockJSON) MarshalToString(v any) (string, error) {
	m.called++
	return "", nil
}

func (m *mockJSON) NewDecoder(r io.Reader) Decoder {
	m.called++
	return nil
}

func (m *mockJSON) NewEncoder(w io.Writer) Encoder {
	m.called++
	return nil
}

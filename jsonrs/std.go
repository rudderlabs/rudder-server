package jsonrs

import (
	"encoding/json"
	"io"
)

// stdJSON is the JSON implementation of package/json.
type stdJSON struct{}

func (j *stdJSON) Marshal(v any) ([]byte, error) {
	return json.Marshal(v) // nolint: forbidigo
}

func (j *stdJSON) MarshalIndent(v any, prefix, indent string) ([]byte, error) {
	return json.MarshalIndent(v, prefix, indent) // nolint: forbidigo
}

func (j *stdJSON) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v) // nolint: forbidigo
}

func (j *stdJSON) MarshalToString(v any) (string, error) {
	m, err := j.Marshal(v) // nolint: forbidigo
	if err != nil {
		return "", err
	}
	return string(m), nil
}

func (j *stdJSON) NewDecoder(r io.Reader) Decoder {
	return json.NewDecoder(r) // nolint: forbidigo
}

func (j *stdJSON) NewEncoder(w io.Writer) Encoder {
	return json.NewEncoder(w) // nolint: forbidigo
}

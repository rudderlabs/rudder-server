package jsonrs

import (
	"io"

	jsoniter "github.com/json-iterator/go"
)

// jsoniterJSON is the JSON implementation of github.com/json-iterator/go.
type jsoniterJSON struct{}

func (j *jsoniterJSON) Marshal(v any) ([]byte, error) {
	return jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(v)
}

func (j *jsoniterJSON) MarshalIndent(v any, prefix, indent string) ([]byte, error) {
	return jsoniter.ConfigCompatibleWithStandardLibrary.MarshalIndent(v, prefix, indent)
}

func (j *jsoniterJSON) Unmarshal(data []byte, v any) error {
	return jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(data, v)
}

func (j *jsoniterJSON) MarshalToString(v any) (string, error) {
	return jsoniter.ConfigCompatibleWithStandardLibrary.MarshalToString(v)
}

func (j *jsoniterJSON) NewDecoder(r io.Reader) Decoder {
	return jsoniter.ConfigCompatibleWithStandardLibrary.NewDecoder(r)
}

func (j *jsoniterJSON) NewEncoder(w io.Writer) Encoder {
	return jsoniter.ConfigCompatibleWithStandardLibrary.NewEncoder(w)
}

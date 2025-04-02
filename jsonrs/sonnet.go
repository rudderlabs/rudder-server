package jsonrs

import (
	"io"

	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/sonnet"
)

// sonnetJSON is the JSON implementation of github.com/rudderlabs/sonnet.
type sonnetJSON struct{}

func (j *sonnetJSON) Marshal(v any) ([]byte, error) {
	return sonnet.Marshal(v)
}

func (j *sonnetJSON) MarshalIndent(v any, prefix, indent string) ([]byte, error) {
	return sonnet.MarshalIndent(v, prefix, indent)
}

func (j *sonnetJSON) Unmarshal(data []byte, v any) error {
	return sonnet.Unmarshal(data, v)
}

func (j *sonnetJSON) MarshalToString(v any) (string, error) {
	m, err := j.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(m), nil
}

func (j *sonnetJSON) NewDecoder(r io.Reader) Decoder {
	// We are always using jsoniter for streaming due to: https://github.com/go-json-experiment/jsonbench?tab=readme-ov-file#streaming
	return jsoniter.ConfigCompatibleWithStandardLibrary.NewDecoder(r)

	// TODO: if we were to enable sonnet streaming it would be unreliable.
	// Issue is easy to reproduce by running warehouse api tests with sonnet streaming enabled
	// return sonnet.NewDecoder(r)
}

func (j *sonnetJSON) NewEncoder(w io.Writer) Encoder {
	// always using jsoniter for streaming due to: https://github.com/go-json-experiment/jsonbench?tab=readme-ov-file#streaming
	return jsoniter.ConfigCompatibleWithStandardLibrary.NewEncoder(w)

	// TODO: if we were to enable sonnet streaming it would be unreliable.
	// Issue is easy to reproduce by running warehouse api tests with sonnet streaming enabled
	// return sonnet.NewEncoder(w)
}

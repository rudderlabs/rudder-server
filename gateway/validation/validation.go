package validation

import (
	"bytes"
	stdjson "encoding/json"
	"io"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// sanitizeJSON makes a json payload safe for writing into postgres.
// 1. Removes any \u0000 string from the payload
// 2. unmashals and marshals the payload to remove any extra keys
func SanitizeJSON(input []byte) ([]byte, error) {
	v := bytes.ReplaceAll(input, []byte(`\u0000`), []byte(""))
	if len(v) == 0 {
		v = []byte(`{}`)
	}
	var a stdjson.RawMessage
	err := json.Unmarshal(v, &a)
	if err != nil {
		return nil, err
	}
	v, err = json.Marshal(a)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func Valid(data []byte) bool {
	return json.Valid(data)
}

func NewDecoder(reader io.Reader) *jsoniter.Decoder {
	return json.NewDecoder(reader)
}

func NewEncoder(writer io.Writer) *jsoniter.Encoder {
	return json.NewEncoder(writer)
}

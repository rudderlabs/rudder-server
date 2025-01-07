package validation

import (
	"bytes"
	stdjson "encoding/json"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// SanitizeJSON makes a json payload safe for writing into postgres.
// 1. Removes any \u0000 string from the payload
// 2. unmarshals and marshals the payload to remove any extra keys
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

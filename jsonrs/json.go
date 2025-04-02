package jsonrs

import (
	"io"

	"github.com/rudderlabs/rudder-go-kit/config"
)

const (
	// StdLib is the JSON implementation of package/json.
	StdLib = "std"
	// SonnetLib is the JSON implementation of github.com/rudderlabs/sonnet.
	SonnetLib = "sonnet"
	// JsoniterLib is the JSON implementation of github.com/json-iterator/go.
	JsoniterLib = "jsoniter"
	// DefaultLib is the default JSON implementation.
	DefaultLib = SonnetLib
)

var Default = New(config.Default)

type Marshaller interface {
	// Marshal returns the JSON encoding of v.
	Marshal(v any) ([]byte, error)
	// MarshalToString returns the JSON encoding of v as a string.
	MarshalToString(v any) (string, error)
	// MarshalIndent returns the JSON encoding of v with indentation.
	MarshalIndent(v any, prefix, indent string) ([]byte, error)
	// NewEncoder returns a new json encoder that writes to w.
	NewEncoder(w io.Writer) Encoder
}
type Unmarshaller interface {
	// Unmarshal parses the JSON-encoded data and stores the result in the value pointed to by v.
	Unmarshal(data []byte, v any) error
	// NewDecoder returns a new json decoder that reads from r.
	NewDecoder(r io.Reader) Decoder
}

// JSON is the interface that wraps the basic JSON operations.
type JSON interface {
	Marshaller
	Unmarshaller
}

// Decoder is the interface that wraps the basic JSON decoder operations.
type Decoder interface {
	Buffered() io.Reader
	Decode(v any) error
	DisallowUnknownFields()
	More() bool
	UseNumber()
}

// Encoder is the interface that wraps the basic JSON encoder operations.
type Encoder interface {
	Encode(v any) error
	SetEscapeHTML(on bool)
	SetIndent(prefix, indent string)
}

// New returns a new JSON implementation based on the configuration.
func New(conf *config.Config) JSON {
	marshaller := conf.GetReloadableStringVar(DefaultLib, "Json.Library.Marshaller", "Json.Library").Load
	unmarshaller := conf.GetReloadableStringVar(DefaultLib, "Json.Library.Unmarshaller", "Json.Library").Load
	return &switcher{
		impls: map[string]JSON{
			StdLib:      &stdJSON{},
			SonnetLib:   &sonnetJSON{},
			JsoniterLib: &jsoniterJSON{},
		},
		marshallerFn:   marshaller,
		unmarshallerFn: unmarshaller,
	}
}

// NewWithLibrary returns a new JSON implementation based on the library.
func NewWithLibrary(library string) JSON {
	switch library {
	case StdLib:
		return &stdJSON{}
	case SonnetLib:
		return &sonnetJSON{}
	case JsoniterLib:
		return &jsoniterJSON{}
	default:
		return &sonnetJSON{}
	}
}

// Reset resets the default JSON implementation based on the default configuration.
func Reset() {
	Default = New(config.Default)
}

// Marshal returns the JSON encoding of v. Uses the default JSON implementation.
func Marshal(v any) ([]byte, error) {
	return Default.Marshal(v)
}

// MarshalIndent returns the JSON encoding of v with indentation. Uses the default JSON implementation.
func MarshalIndent(v any, prefix, indent string) ([]byte, error) {
	return Default.MarshalIndent(v, prefix, indent)
}

// Unmarshal parses the JSON-encoded data and stores the result in the value pointed to by v. Uses the default JSON implementation.
func Unmarshal(data []byte, v any) error {
	return Default.Unmarshal(data, v)
}

// MarshalToString returns the JSON encoding of v as a string. Uses the default JSON implementation.
func MarshalToString(v any) (string, error) {
	return Default.MarshalToString(v)
}

// NewDecoder returns a new json decoder that reads from r. Uses the default JSON implementation.
func NewDecoder(r io.Reader) Decoder {
	return Default.NewDecoder(r)
}

// NewEncoder returns a new json encoder that writes to w. Uses the default JSON implementation.
func NewEncoder(w io.Writer) Encoder {
	return Default.NewEncoder(w)
}

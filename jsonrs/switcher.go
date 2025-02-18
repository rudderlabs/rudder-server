package jsonrs

import "io"

// switcher is a JSON implementation that switches between different JSON implementations, based on the configuration.
type switcher struct {
	marshaller   func() string
	unmarshaller func() string
	impls        map[string]JSON
}

func (s *switcher) Marshal(v any) ([]byte, error) {
	return s.impl().Marshal(v)
}

func (s *switcher) MarshalIndent(v any, prefix, indent string) ([]byte, error) {
	return s.impl().MarshalIndent(v, prefix, indent)
}

func (s *switcher) Unmarshal(data []byte, v any) error {
	return s.impl().Unmarshal(data, v)
}

func (s *switcher) MarshalToString(v any) (string, error) {
	return s.impl().MarshalToString(v)
}

func (s *switcher) NewDecoder(r io.Reader) Decoder {
	return s.impl().NewDecoder(r)
}

func (s *switcher) NewEncoder(w io.Writer) Encoder {
	return s.impl().NewEncoder(w)
}

func (s *switcher) impl() JSON {
	if impl, ok := s.impls[s.marshaller()]; ok {
		return impl
	}
	return s.impls[DefaultLib]
}

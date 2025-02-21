package jsonrs

import "io"

// switcher is a JSON implementation that switches between different JSON implementations, based on the configuration.
type switcher struct {
	marshallerFn   func() string
	unmarshallerFn func() string
	impls          map[string]JSON
}

func (s *switcher) Marshal(v any) ([]byte, error) {
	return s.marshaller().Marshal(v)
}

func (s *switcher) MarshalIndent(v any, prefix, indent string) ([]byte, error) {
	return s.marshaller().MarshalIndent(v, prefix, indent)
}

func (s *switcher) Unmarshal(data []byte, v any) error {
	return s.unmarshaller().Unmarshal(data, v)
}

func (s *switcher) MarshalToString(v any) (string, error) {
	return s.marshaller().MarshalToString(v)
}

func (s *switcher) NewDecoder(r io.Reader) Decoder {
	return s.unmarshaller().NewDecoder(r)
}

func (s *switcher) NewEncoder(w io.Writer) Encoder {
	return s.marshaller().NewEncoder(w)
}

func (s *switcher) marshaller() Marshaller {
	if impl, ok := s.impls[s.marshallerFn()]; ok {
		return impl
	}
	return s.impls[DefaultLib]
}

func (s *switcher) unmarshaller() Unmarshaller {
	if impl, ok := s.impls[s.unmarshallerFn()]; ok {
		return impl
	}
	return s.impls[DefaultLib]
}

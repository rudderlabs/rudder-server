package misc

// DefaultString is a utility type for providing default values using one-liners
// in otherwise multi-line scenarios.
//
// E.g. this
//
//	v := misc.DefaultString("unknown").OnError(os.Hostname())
//
// is the equivalent of
//
//	v, err := os.Hostname()
//	if err != nil {
//	  v = 	"unknown"
//	}
type DefaultString string

// OnError returns the default value if the err argument is not nil, otherwise the value
func (r DefaultString) OnError(value string, err error) string {
	if err != nil {
		return string(r)
	}
	return value
}

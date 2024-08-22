package crash

import "net/http"

type NOOP struct{}

func (*NOOP) Notify(team string) func() {
	return func() {}
}

func (*NOOP) Handler(h http.Handler) http.Handler {
	return h
}

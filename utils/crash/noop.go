package crash

import "net/http"

type NOOP struct{}

func (n *NOOP) Notify(team string) func() {
	return func() {}
}

func (n *NOOP) Handler(h http.Handler) http.Handler {
	return h
}

package noop

import (
	"net/http"
)

type NoopAuth struct {
}

func (a *NoopAuth) Authenticate(r *http.Request) error {
	return nil
}

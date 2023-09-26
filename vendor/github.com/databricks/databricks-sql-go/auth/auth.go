package auth

import "net/http"

type Authenticator interface {
	Authenticate(*http.Request) error
}

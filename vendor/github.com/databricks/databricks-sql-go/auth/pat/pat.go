package pat

import (
	"fmt"
	"net/http"

	"github.com/pkg/errors"
)

type PATAuth struct {
	AccessToken string
}

func (a *PATAuth) Authenticate(r *http.Request) error {
	if a.AccessToken == "" {
		return errors.New("invalid token")
	}
	r.Header.Set("Authorization", fmt.Sprintf("Bearer %s", a.AccessToken))
	return nil
}

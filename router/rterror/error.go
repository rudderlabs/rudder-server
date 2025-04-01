package rterror

import (
	"errors"
)

var ErrDisabledEgress = errors.New("200: outgoing disabled")

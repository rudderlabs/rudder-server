package rterror

import (
	"errors"
)

var DisabledEgress = errors.New("200: outgoing disabled")

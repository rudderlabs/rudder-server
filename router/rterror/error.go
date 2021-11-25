package rterror

import (
	"errors"
)

var (
	DisabledEgress         = errors.New("200: outgoing disabled")
	InvalidServiceProvider = errors.New("service provider not supported")
)

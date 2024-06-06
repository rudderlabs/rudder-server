package noop

import (
	ht "github.com/rudderlabs/rudder-server/helper/types"
)

type NoopHandler struct{}

func New() (*NoopHandler, error) {
	return &NoopHandler{}, nil
}

func (*NoopHandler) Send(input, output any, metainfo ht.MetaInfo) {}

func (*NoopHandler) Shutdown() {}

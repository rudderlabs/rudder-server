package noop

import "github.com/rudderlabs/rudder-server/helper"

type NoopHandler struct{}

func New() (*NoopHandler, error) {
	return &NoopHandler{}, nil
}

func (*NoopHandler) Send(input, output any, metainfo helper.MetaInfo) {}

func (*NoopHandler) Shutdown() {}

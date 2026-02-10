// Package usertransformer re-exports the internal user_transformer package
// so it can be used from integration tests outside the processor/ tree.
package usertransformer

import (
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	internal "github.com/rudderlabs/rudder-server/processor/internal/transformer/user_transformer"
)

type Client = internal.Client

type Opt = internal.Opt

func New(conf *config.Config, log logger.Logger, stat stats.Stats, opts ...Opt) *Client {
	return internal.New(conf, log, stat, opts...)
}

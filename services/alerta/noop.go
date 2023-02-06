package alerta

import (
	"context"
)

// NOOP alerta implementation that does nothing
type NOOP struct{}

func (*NOOP) SendAlert(context.Context, string, SendAlertOpts) error {
	return nil
}

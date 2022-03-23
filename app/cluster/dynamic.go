package cluster

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/types/servermode"
)

type modeProvider interface {
	ServerMode() <-chan servermode.ModeAck
}

type Lifecycle interface {
	Start()
	Stop()
}

type Dynamic struct {
	Provider modeProvider

	GatewayDB     Lifecycle
	RouterDB      Lifecycle
	BatchRouterDB Lifecycle
	ErrorDB       Lifecycle

	Processor Lifecycle
	Router    Lifecycle

	currentMode servermode.Mode
}

func (d *Dynamic) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			if d.currentMode == servermode.NormalMode {
				d.stop()
			}
			return nil
		case newMode := <-d.Provider.ServerMode():
			err := d.handleModeChange(newMode.Mode())
			if err != nil {
				return err
			}
			newMode.Ack()
		}
	}
}

func (d *Dynamic) start() {
	d.ErrorDB.Start()
	d.GatewayDB.Start()
	d.RouterDB.Start()
	d.BatchRouterDB.Start()

	d.Processor.Start()
	d.Router.Start()
}

func (d *Dynamic) stop() {
	d.Processor.Stop()
	d.Router.Stop()

	d.RouterDB.Stop()
	d.BatchRouterDB.Stop()
	d.ErrorDB.Stop()
	d.GatewayDB.Stop()
}

func (d *Dynamic) handleModeChange(newMode servermode.Mode) error {
	if d.currentMode == newMode {
		// TODO add logging
		return nil
	}
	switch d.currentMode {
	case servermode.UndefinedMode:
		switch newMode {
		case servermode.NormalMode:
			d.start()
		case servermode.DegradedMode:

		default:
			return fmt.Errorf("unsupported transition from UndefinedMode to %s", newMode)
		}
	case servermode.NormalMode:
		switch newMode {
		case servermode.DegradedMode:
			d.stop()
		default:
			return fmt.Errorf("unsupported transition from NormalMode to %s", newMode)
		}
	case servermode.DegradedMode:
		switch newMode {
		case servermode.NormalMode:
			d.start()
		default:
			return fmt.Errorf("unsupported transition from DegradedMode to %s", newMode)
		}
	}

	d.currentMode = newMode
	return nil
}

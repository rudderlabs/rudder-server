package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"
)

var (
	controller     = "ETCD"
	controllerType = "Dynamic"
)

type ChangeEventProvider interface {
	ServerMode(ctx context.Context) <-chan servermode.ChangeEvent
}

type lifecycle interface {
	Start() error
	Stop()
}

type Dynamic struct {
	Provider ChangeEventProvider

	GatewayComponent bool

	GatewayDB     lifecycle
	RouterDB      lifecycle
	BatchRouterDB lifecycle
	ErrorDB       lifecycle
	EventSchemaDB lifecycle
	ArchivalDB    lifecycle

	Processor lifecycle
	Router    lifecycle

	SchemaForwarder lifecycle
	Archiver        lifecycle

	currentMode servermode.Mode

	serverStartTimeStat  stats.Measurement
	serverStopTimeStat   stats.Measurement
	serverStartCountStat stats.Measurement
	serverStopCountStat  stats.Measurement

	logger logger.Logger

	once sync.Once
}

func (d *Dynamic) init() {
	d.currentMode = servermode.DegradedMode
	d.logger = logger.NewLogger().Child("cluster")
	tag := stats.Tags{
		"controlled_by":   controller,
		"controller_type": controllerType,
	}
	d.serverStartTimeStat = stats.Default.NewTaggedStat("cluster.server_start_time", stats.TimerType, tag)
	d.serverStopTimeStat = stats.Default.NewTaggedStat("cluster.server_stop_time", stats.TimerType, tag)
	d.serverStartCountStat = stats.Default.NewTaggedStat("cluster.server_start_count", stats.CountType, tag)
	d.serverStopCountStat = stats.Default.NewTaggedStat("cluster.server_stop_count", stats.CountType, tag)
}

func (d *Dynamic) Run(ctx context.Context) error {
	d.once.Do(d.init)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	serverModeChan := d.Provider.ServerMode(ctx)
	if d.GatewayComponent {
		d.currentMode = servermode.NormalMode
	}

	if len(serverModeChan) == 0 {
		d.logger.Info("No server mode change event received. Starting server in normal mode")
		if err := d.handleModeChange(servermode.NormalMode); err != nil {
			return err
		}
	}
	for {
		select {
		case <-ctx.Done():
			if d.currentMode == servermode.NormalMode {
				d.stop()
			}
			return nil
		case req := <-serverModeChan:
			if req.Err() != nil {
				return req.Err()
			}

			d.logger.Infof("Got trigger to change the mode, new mode: %s, old mode: %s", req.Mode(), d.currentMode)
			if d.GatewayComponent {
				d.logger.Infof("Gateway component, not changing the mode")
				continue
			}
			err := d.handleModeChange(req.Mode())
			if err != nil {
				d.logger.Error(err)
				return err
			}
			d.logger.Debugf("Acknowledging the mode change")

			if err := req.Ack(ctx); err != nil {
				return fmt.Errorf("ack mode change: %w", err)
			}
		}
	}
}

func (d *Dynamic) start() error {
	if d.GatewayComponent {
		return nil
	}
	d.logger.Info("Starting the server")
	start := time.Now()
	if err := d.ErrorDB.Start(); err != nil {
		return fmt.Errorf("error db start: %w", err)
	}
	if err := d.GatewayDB.Start(); err != nil {
		return fmt.Errorf("gateway db start: %w", err)
	}
	if err := d.EventSchemaDB.Start(); err != nil {
		return fmt.Errorf("event schemas db start: %w", err)
	}
	if err := d.ArchivalDB.Start(); err != nil {
		return fmt.Errorf("archival db start: %w", err)
	}
	if err := d.RouterDB.Start(); err != nil {
		return fmt.Errorf("router db start: %w", err)
	}
	if err := d.BatchRouterDB.Start(); err != nil {
		return fmt.Errorf("batch router db start: %w", err)
	}
	if err := d.Processor.Start(); err != nil {
		return fmt.Errorf("processor start: %w", err)
	}
	if err := d.Archiver.Start(); err != nil {
		return fmt.Errorf("archiver start: %w", err)
	}
	if err := d.SchemaForwarder.Start(); err != nil {
		return fmt.Errorf("jobs forwarder start: %w", err)
	}
	if err := d.Router.Start(); err != nil {
		return fmt.Errorf("router start: %w", err)
	}
	d.serverStartTimeStat.SendTiming(time.Since(start))
	d.serverStartCountStat.Increment()
	return nil
}

func (d *Dynamic) stop() {
	if d.GatewayComponent {
		d.logger.Info("Stopping the gateway")
		return
	}
	d.logger.Info("Stopping the server")
	start := time.Now()
	d.Router.Stop()
	d.logger.Debug("Router stopped")
	d.SchemaForwarder.Stop()
	d.logger.Debug("JobsForwarder stopped")
	d.Archiver.Stop()
	d.logger.Debug("Archiver stopped")
	d.Processor.Stop()
	d.logger.Debug("Processor stopped")

	d.BatchRouterDB.Stop()
	d.logger.Debug("BatchRouterDB stopped")
	d.RouterDB.Stop()
	d.logger.Debug("RouterDB stopped")
	d.EventSchemaDB.Stop()
	d.logger.Debug("EventSchemasDB stopped")
	d.ArchivalDB.Stop()
	d.logger.Debug("ArchivalDB stopped")
	d.GatewayDB.Stop()
	d.logger.Debug("GatewayDB stopped")
	d.ErrorDB.Stop()
	d.logger.Debug("ErrorDB stopped")
	d.serverStopTimeStat.Since(start)
	d.serverStopCountStat.Increment()
}

func (d *Dynamic) handleModeChange(newMode servermode.Mode) error {
	if d.GatewayComponent {
		d.logger.Info("Not transiting the server because this is only Gateway App")
		return nil
	}
	if !newMode.Valid() {
		return fmt.Errorf("unsupported mode: %s", newMode)
	}

	if d.currentMode == newMode {
		d.logger.Info("New mode is same as old mode: %s, not switching the mode.", string(newMode))
		return nil
	}
	switch d.currentMode {
	case servermode.NormalMode:
		switch newMode {
		case servermode.DegradedMode:
			d.logger.Info("Transiting the server from NormalMode to DegradedMode")
			d.stop()
		default:
			d.logger.Errorf("Unsupported transition from NormalMode to %s", newMode)
			return fmt.Errorf("unsupported transition from NormalMode to %s", newMode)
		}
	case servermode.DegradedMode:
		switch newMode {
		case servermode.NormalMode:
			d.logger.Info("Transiting the server from DegradedMode to NormalMode")
			if err := d.start(); err != nil {
				d.logger.Errorf("Failed to start the server: %v", err)
				return fmt.Errorf("failed to start the server: %w", err)
			}
		default:
			d.logger.Errorf("Unsupported transition from DegradedMode to %s", newMode)
			return fmt.Errorf("unsupported transition from DegradedMode to %s", newMode)
		}
	}

	d.currentMode = newMode
	return nil
}

func (d *Dynamic) Mode() servermode.Mode {
	return d.currentMode
}

package cluster

import (
	"context"
	"fmt"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/utils/types/servermode"
)

var (
	controller     string = "ETCD"
	controllertype string = "Dynamic"
)

type ModeProvider interface {
	ServerMode() (<-chan servermode.Ack, error)
	Close()
}

type lifecycle interface {
	Start()
	Stop()
}

type Dynamic struct {
	Provider ModeProvider

	GatewayDB     lifecycle
	RouterDB      lifecycle
	BatchRouterDB lifecycle
	ErrorDB       lifecycle

	Processor lifecycle
	Router    lifecycle

	MultiTenantStat lifecycle

	currentMode servermode.Mode

	serverStartTimeStat  stats.RudderStats
	serverStopTimeStat   stats.RudderStats
	serverStartCountStat stats.RudderStats
	serverStopCountStat  stats.RudderStats

	logger logger.LoggerI

	once sync.Once
}

func (d *Dynamic) init() {
	d.currentMode = servermode.DegradedMode
	d.logger = logger.NewLogger().Child("cluster")
	tag := stats.Tags{
		"controlled_by":   controller,
		"controller_type": controllertype,
	}
	d.serverStartTimeStat = stats.NewTaggedStat("cluster.server_start_time", stats.TimerType, tag)
	d.serverStopTimeStat = stats.NewTaggedStat("cluster.server_stop_time", stats.TimerType, tag)
	d.serverStartCountStat = stats.NewTaggedStat("cluster.server_start_count", stats.CountType, tag)
	d.serverStopCountStat = stats.NewTaggedStat("cluster.server_stop_count", stats.CountType, tag)
}

func (d *Dynamic) Run(ctx context.Context) error {
	d.once.Do(func() {
		d.init()
	})
	defer d.Provider.Close()
	modeChan, err := d.Provider.ServerMode()
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			if d.currentMode == servermode.NormalMode {
				d.stop()
			}
			return nil
		case newMode := <-modeChan:
			d.logger.Infof("Got trigger to change the mode, new mode: %s, old mode: %s", newMode.Mode(), d.currentMode)
			err = d.handleModeChange(newMode.Mode())
			if err != nil {
				d.logger.Error(err)
				return err
			}
			d.logger.Debugf("Acknowledging the mode change.")
			newMode.Ack()
		}
	}
}

func (d *Dynamic) start() {
	d.logger.Info("Starting the server")
	start := time.Now()
	d.ErrorDB.Start()
	d.GatewayDB.Start()
	d.RouterDB.Start()
	d.BatchRouterDB.Start()

	d.MultiTenantStat.Start()

	d.Processor.Start()
	d.Router.Start()
	d.serverStartTimeStat.SendTiming(time.Since(start))
	d.serverStartCountStat.Increment()
}

func (d *Dynamic) stop() {
	d.logger.Info("Stopping the server")
	start := time.Now()
	d.serverStopTimeStat.Start()
	d.Processor.Stop()
	d.Router.Stop()
	d.MultiTenantStat.Stop()

	d.RouterDB.Stop()
	d.BatchRouterDB.Stop()
	d.ErrorDB.Stop()
	d.GatewayDB.Stop()
	d.serverStopTimeStat.SendTiming(time.Since(start))
	d.serverStopCountStat.Increment()
}

func (d *Dynamic) handleModeChange(newMode servermode.Mode) error {
	if !newMode.Valid() {
		return fmt.Errorf("unsupported mode: %s", newMode)
	}
	if d.currentMode == newMode {
		// TODO add logging
		return nil
	}
	switch d.currentMode {
	case servermode.NormalMode:
		switch newMode {
		case servermode.DegradedMode:
			d.logger.Info("Transiting the server from NormalMode to DegradedMode")
			d.stop()
		default:
			d.logger.Errorf("Unsupported transition from NormalMode to %s \n", newMode)
			return fmt.Errorf("unsupported transition from NormalMode to %s", newMode)
		}
	case servermode.DegradedMode:
		switch newMode {
		case servermode.NormalMode:
			d.logger.Info("Transiting the server from DegradedMode to NormalMode")
			d.start()
		default:
			d.logger.Errorf("Unsupported transition from DegradedMode to %s \n", newMode)
			return fmt.Errorf("unsupported transition from DegradedMode to %s", newMode)
		}
	}

	d.currentMode = newMode
	return nil
}

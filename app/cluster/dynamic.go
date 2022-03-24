package cluster

import (
	"context"
	"fmt"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"time"

	"github.com/rudderlabs/rudder-server/utils/types/servermode"
)

var (
	pkgLogger logger.LoggerI
	controller string = "ETCD"
	controllertype string = "Dynamic"
)

func Init() {
	pkgLogger = logger.NewLogger().Child("cluster")
}

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

	serverStartTimeStat  stats.RudderStats
	serverStopTimeStat   stats.RudderStats
	serverStartCountStat stats.RudderStats
	serverStopCountStat  stats.RudderStats

	logger logger.LoggerI
}

func (d *Dynamic) Setup()  {
	d.currentMode = servermode.DegradedMode
	d.logger = pkgLogger
	tag := stats.Tags{
		"controlled_by": controller,
		"controller_type": controllertype,
	}
	d.serverStartTimeStat = stats.NewTaggedStat("cluster.server_start_time", stats.TimerType, tag)
	d.serverStopTimeStat = stats.NewTaggedStat("cluster.server_stop_time", stats.TimerType, tag)
	d.serverStartCountStat = stats.NewTaggedStat("cluster.server_start_count", stats.CountType, tag)
	d.serverStopCountStat = stats.NewTaggedStat("cluster.server_stop_count", stats.CountType, tag)
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
			d.logger.Debugf("Got trigger to change the mode, new mode: %s, old mode: %s", newMode.Mode(), d.currentMode)
			err := d.handleModeChange(newMode.Mode())
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

	d.RouterDB.Stop()
	d.BatchRouterDB.Stop()
	d.ErrorDB.Stop()
	d.GatewayDB.Stop()
	d.serverStopTimeStat.SendTiming(time.Since(start))
	d.serverStopCountStat.Increment()
}

func (d *Dynamic) handleModeChange(newMode servermode.Mode) error {
	if d.currentMode == newMode {
		// TODO add logging
		return nil
	}
	switch d.currentMode {
	//case servermode.UndefinedMode:
	//	switch newMode {
	//	case servermode.NormalMode:
	//		d.logger.Info("Transiting the server from UndefinedMode to NormalMode")
	//		d.start()
	//	case servermode.DegradedMode:
	//		d.logger.Info("Server is running in UndefinedMode, can not transit to DegradedMode.")
	//	default:
	//		d.logger.Errorf("Unsupported transition from UndefinedMode to %s \n", newMode)
	//		return fmt.Errorf("unsupported transition from UndefinedMode to %s", newMode)
	//	}
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

package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"

	"github.com/rudderlabs/rudder-server/utils/types/servermode"
	"github.com/rudderlabs/rudder-server/utils/types/workspace"
)

var (
	controller     string = "ETCD"
	controllertype string = "Dynamic"
)

type ModeProvider interface {
	ServerMode(ctx context.Context) <-chan servermode.ModeRequest
	WorkspaceIDs(ctx context.Context) <-chan workspace.WorkspacesRequest
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

	currentMode         servermode.Mode
	currentWorkspaceIDs string

	serverStartTimeStat  stats.RudderStats
	serverStopTimeStat   stats.RudderStats
	serverStartCountStat stats.RudderStats
	serverStopCountStat  stats.RudderStats
	backendConfig        backendconfig.BackendConfig

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
	d.backendConfig = backendconfig.DefaultBackendConfig
}

func (d *Dynamic) Run(ctx context.Context) error {
	// workspacesChan := d.Provider.WorkspaceServed()
	d.once.Do(d.init)

	serverModeChan := d.Provider.ServerMode(ctx)
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

			d.logger.Debugf("Got trigger to change the mode, new mode: %s, old mode: %s", req.Mode(), d.currentMode)
			err := d.handleModeChange(req.Mode())
			if err != nil {
				d.logger.Error(err)
				return err
			}
			d.logger.Debugf("Acknowledging the mode change.")
			req.Ack()
			// case newWorkspaces := <-workspacesChan:
			// 	err := d.handleWorkspaceChange(newWorkspaces.Workspaces())
			// 	if err != nil {
			// 		d.logger.Error(err)
			// 		return err
			// 	}
			// 	newWorkspaces.Ack()
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

func (d *Dynamic) handleWorkspaceChange(workspaces string) error {
	if d.currentWorkspaceIDs == workspaces {
		return nil
	}
	d.backendConfig.StopPolling()
	d.backendConfig.StartPolling(workspaces)
	return nil
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

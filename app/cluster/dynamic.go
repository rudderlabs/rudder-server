package cluster

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"

	"github.com/rudderlabs/rudder-server/utils/types/servermode"
	"github.com/rudderlabs/rudder-server/utils/types/workspace"
)

var (
	controller     = "ETCD"
	controllerType = "Dynamic"
)

type ChangeEventProvider interface {
	ServerMode(ctx context.Context) <-chan servermode.ChangeEvent
	WorkspaceIDs(ctx context.Context) <-chan workspace.ChangeEvent
}

type lifecycle interface {
	Start()
	Stop()
}

type configLifecycle interface {
	Stop()
	StartWithIDs(ctx context.Context, workspaces string)
	WaitForConfig(ctx context.Context) error
}

type Dynamic struct {
	Provider ChangeEventProvider

	GatewayComponent bool

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
	BackendConfig        configLifecycle

	logger logger.LoggerI

	once sync.Once
}

func (d *Dynamic) init() {
	d.currentMode = servermode.DegradedMode
	d.logger = logger.NewLogger().Child("cluster")
	tag := stats.Tags{
		"controlled_by":   controller,
		"controller_type": controllerType,
	}
	d.serverStartTimeStat = stats.NewTaggedStat("cluster.server_start_time", stats.TimerType, tag)
	d.serverStopTimeStat = stats.NewTaggedStat("cluster.server_stop_time", stats.TimerType, tag)
	d.serverStartCountStat = stats.NewTaggedStat("cluster.server_start_count", stats.CountType, tag)
	d.serverStopCountStat = stats.NewTaggedStat("cluster.server_stop_count", stats.CountType, tag)

	if d.BackendConfig == nil {
		d.BackendConfig = backendconfig.DefaultBackendConfig
	}
}

func (d *Dynamic) Run(ctx context.Context) error {
	d.once.Do(d.init)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	serverModeChan := d.Provider.ServerMode(ctx)
	workspaceIDsChan := d.Provider.WorkspaceIDs(ctx)
	if d.GatewayComponent {
		d.currentMode = servermode.NormalMode
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
			err := d.handleModeChange(req.Mode())
			if err != nil {
				d.logger.Error(err)
				return err
			}
			d.logger.Debugf("Acknowledging the mode change")

			if err := req.Ack(ctx); err != nil {
				return fmt.Errorf("ack mode change: %w", err)
			}
		case req := <-workspaceIDsChan:
			if req.Err() != nil {
				return req.Err()
			}
			ids := strings.Join(req.WorkspaceIDs(), ",")

			d.logger.Infof("Got trigger to change workspaceIDs: %q", ids)
			err := d.handleWorkspaceChange(ctx, ids)
			if err != nil {
				d.logger.Debugf("Could not handle workspaceIDs change: %v", err)
				if ackErr := req.AckWithError(ctx, err); ackErr != nil {
					return fmt.Errorf("ack workspaceIDs change with error: %v: %w", err, ackErr)
				}
				return err
			}

			d.logger.Debugf("Acknowledging the workspaceIDs change")
			if err := req.Ack(ctx); err != nil {
				return fmt.Errorf("ack workspaceIDs change: %w", err)
			}
		}
	}
}

func (d *Dynamic) start() {
	if d.GatewayComponent {
		return
	}
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
	if d.GatewayComponent {
		d.logger.Info("Stopping the gateway")
		return
	}
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

func (d *Dynamic) handleWorkspaceChange(ctx context.Context, workspaces string) error {
	if d.currentWorkspaceIDs == workspaces {
		return nil
	}
	d.BackendConfig.Stop()
	d.BackendConfig.StartWithIDs(ctx, workspaces)
	d.currentWorkspaceIDs = workspaces
	return d.BackendConfig.WaitForConfig(ctx)
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

func (d *Dynamic) Mode() servermode.Mode {
	return d.currentMode
}

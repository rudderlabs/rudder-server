package cpservice

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/controlplane"
	proto "github.com/rudderlabs/rudder-server/proto/processor"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

// ShouldConnect reports whether this pod should open the cp-router connection:
// the feature must be enabled AND this must be the node-0 pod. Every other pod
// (and the flag-off case) skips the bootstrap entirely, so cp-router holds a
// single inbound connection per dataplane and node-0 is the sole owner of any
// privileged work done over it.
func ShouldConnect(conf *config.Config) bool {
	return conf.GetBoolVar(false, "Processor.cpRouterConnection.enabled") && isNode0()
}

// isNode0 reports whether this pod is the node-0 pod, using the same instance-ID
// ordinal signal as app/cluster/state. misc.GetInstanceID already returns the
// trailing ordinal of INSTANCE_ID ("0", "1", …); an unset INSTANCE_ID yields ""
// (not node-0), so local runs never connect.
func isNode0() bool {
	return misc.GetInstanceID() == "0"
}

// Connector owns the cp-router dataplane connection for the processor. It wraps
// the shared controlplane.ConnectionManager and drives it from the workspace
// connection flags published by the control plane (the warehouse pattern).
type Connector struct {
	log           logger.Logger
	backendConfig backendconfig.BackendConfig
	cm            *controlplane.ConnectionManager
	lastURL       string
}

// NewConnector builds the cp-router connection for the processor, registering
// the given ProcessorService handler on the dataplane-side gRPC server. It does
// not dial until Run observes a connection flag enabling the service.
func NewConnector(
	conf *config.Config,
	log logger.Logger,
	backendConfig backendconfig.BackendConfig,
	handler proto.ProcessorServiceServer,
) (*Connector, error) {
	connectionToken, tokenType, _, err := deployment.GetConnectionToken()
	if err != nil {
		return nil, fmt.Errorf("getting connection token: %w", err)
	}

	labels := map[string]string{}
	if region := conf.GetStringVar("", "region"); region != "" {
		labels["region"] = region
	}

	log = log.Child("cpservice")
	cm := &controlplane.ConnectionManager{
		AuthInfo: controlplane.AuthInfo{
			Service:         ServiceName,
			ConnectionToken: connectionToken,
			InstanceID:      conf.GetStringVar("1", "INSTANCE_ID"),
			TokenType:       tokenType,
			Labels:          labels,
		},
		RetryInterval:   conf.GetDurationVar(0, time.Second, "Processor.cpRouterConnection.retryInterval"),
		UseTLS:          conf.GetBoolVar(true, "CP_ROUTER_USE_TLS"),
		Logger:          log,
		RegisterService: func(srv *grpc.Server) { proto.RegisterProcessorServiceServer(srv, handler) },
	}
	return &Connector{log: log, backendConfig: backendConfig, cm: cm}, nil
}

// Run subscribes to backend config and keeps the cp-router connection in sync
// with the workspace connection flags until ctx is cancelled, at which point it
// closes the connection. It blocks, so run it in the app errgroup.
func (c *Connector) Run(ctx context.Context) error {
	defer c.stop()
	ch := c.backendConfig.Subscribe(ctx, backendconfig.TopicBackendConfig)
	for {
		select {
		case <-ctx.Done():
			return nil
		case data, ok := <-ch:
			if !ok {
				return nil
			}
			if configData, ok := data.Data.(map[string]backendconfig.ConfigT); ok {
				c.apply(configData)
			}
		}
	}
}

// apply reads the cp-router connection flag for this service from the workspace
// config and applies it. The flags are identical across workspaces in a
// multi-workspace deployment, so the first one carrying the flag is enough.
func (c *Connector) apply(configData map[string]backendconfig.ConfigT) {
	for _, wConfig := range configData {
		flags := wConfig.ConnectionFlags
		if active, ok := flags.Services[ServiceName]; ok {
			c.lastURL = flags.URL
			c.cm.Apply(flags.URL, active)
			return
		}
	}
}

func (c *Connector) stop() {
	if c.lastURL != "" {
		c.cm.Apply(c.lastURL, false)
	}
}

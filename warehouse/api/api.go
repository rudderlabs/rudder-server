package api

import (
	"context"
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/notifier"
	"github.com/rudderlabs/rudder-server/warehouse/bcm"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/jobs"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
)

type Api struct {
	httpServer *httpServer
	grpcServer *grpcServer
}

func New(
	mode string,
	conf *config.Config,
	logger logger.Logger,
	statsFactory stats.Stats,
	bcConfig backendconfig.BackendConfig,
	db *sqlmw.DB,
	notifier *notifier.Notifier,
	tenantManager *multitenant.Manager,
	bcManager *bcm.BackendConfigManager,
	asyncManager *jobs.AsyncJobWh,
	triggerStore *sync.Map,
) (*Api, error) {
	a := &Api{}
	a.httpServer = newhttpServer(
		mode,
		conf,
		logger,
		statsFactory,
		bcConfig,
		db,
		notifier,
		tenantManager,
		bcManager,
		asyncManager,
		triggerStore,
	)

	var err error
	a.grpcServer, err = newGRPCServer(
		conf,
		logger,
		db,
		tenantManager,
		bcManager,
		triggerStore,
	)
	if err != nil {
		return nil, fmt.Errorf("creating grpc server: %v", err)
	}
	return a, nil
}

func (a *Api) StartHTTPServer(ctx context.Context) error {
	return a.httpServer.start(ctx)
}

func (a *Api) StartGRPCServer(ctx context.Context) {
	a.grpcServer.start(ctx)
}

package router

import (
	"context"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/services/multitenant"
	"github.com/rudderlabs/rudder-server/utils/types"
	"time"
)

type Router struct {
	rt                *Factory
	brt               *batchrouter.Factory
	mainCtx           context.Context
	gatewayDB         *jobsdb.HandleT
	routerDB          *jobsdb.MultiTenantJobsDB
	batchRouterDB     *jobsdb.HandleT
	procErrorDB       *jobsdb.HandleT
	gwDBRetention     *time.Duration
	routerDBRetention *time.Duration
	clearDB           *bool
	migrationMode     *string
	multitenantStats  multitenant.MultiTenantI
	reportingI        types.ReportingI
	backendConfig     backendconfig.BackendConfig
	currentCancel     context.CancelFunc
}

func (r *Router) Run(ctx context.Context) error {
	return nil
}

func (r *Router) StartNew() {

}

func (r *Router) Stop() {

}

func NewRouterManager() *Router {
	return &Router{
		rt:                nil,
		brt:               nil,
		mainCtx:           nil,
		gatewayDB:         nil,
		routerDB:          nil,
		batchRouterDB:     nil,
		procErrorDB:       nil,
		gwDBRetention:     nil,
		routerDBRetention: nil,
		clearDB:           nil,
		migrationMode:     nil,
		multitenantStats:  nil,
		reportingI:        nil,
		backendConfig:     nil,
		currentCancel:     nil,
	}
}

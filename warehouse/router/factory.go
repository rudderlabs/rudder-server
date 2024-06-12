package router

import (
	"sync"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/controlplane"
	"github.com/rudderlabs/rudder-server/services/notifier"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/bcm"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
)

type Factory struct {
	reporting          types.Reporting
	conf               *config.Config
	logger             logger.Logger
	statsFactory       stats.Stats
	db                 *sqlquerywrapper.DB
	notifier           *notifier.Notifier
	tenantManager      *multitenant.Manager
	controlPlaneClient *controlplane.Client
	bcManager          *bcm.BackendConfigManager
	encodingFactory    *encoding.Factory
	triggerStore       *sync.Map
	createUploadAlways createUploadAlwaysLoader
}

func (f *Factory) New(destType string) *Router {
	return New(
		f.reporting,
		destType,
		f.conf,
		f.logger.Child("router"),
		f.statsFactory,
		f.db,
		f.notifier,
		f.tenantManager,
		f.controlPlaneClient,
		f.bcManager,
		f.encodingFactory,
		f.triggerStore,
		f.createUploadAlways,
	)
}

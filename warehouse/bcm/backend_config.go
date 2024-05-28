package bcm

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/rudderlabs/rudder-server/warehouse/multitenant"

	"github.com/rudderlabs/rudder-server/warehouse/logfield"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	cpclient "github.com/rudderlabs/rudder-server/warehouse/client/controlplane"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func New(
	c *config.Config,
	db *sqlquerywrapper.DB,
	tenantManager *multitenant.Manager,
	log logger.Logger,
	stats stats.Stats,
) *BackendConfigManager {
	if c == nil {
		c = config.Default
	}
	if log == nil {
		log = logger.NOP
	}
	bcm := &BackendConfigManager{
		conf:                 c,
		db:                   db,
		schema:               repo.NewWHSchemas(db),
		tenantManager:        tenantManager,
		logger:               log,
		stats:                stats,
		InitialConfigFetched: make(chan struct{}),
		connectionsMap:       make(map[string]map[string]model.Warehouse),
	}
	if c.GetBool("ENABLE_TUNNELLING", true) {
		bcm.internalControlPlaneClient = cpclient.NewInternalClientWithCache(
			c.GetString("CONFIG_BACKEND_URL", "https://api.rudderstack.com"),
			cpclient.BasicAuth{
				Username: c.GetString("CP_INTERNAL_API_USERNAME", ""),
				Password: c.GetString("CP_INTERNAL_API_PASSWORD", ""),
			},
		)
	}
	return bcm
}

// BackendConfigManager is used to handle the backend configuration in the Warehouse
type BackendConfigManager struct {
	conf                       *config.Config
	db                         *sqlquerywrapper.DB
	schema                     *repo.WHSchema
	tenantManager              *multitenant.Manager
	internalControlPlaneClient cpclient.InternalControlPlane
	logger                     logger.Logger
	stats                      stats.Stats

	InitialConfigFetched          chan struct{}
	closeInitialConfigFetchedOnce sync.Once

	subscriptions   []chan []model.Warehouse
	subscriptionsMu sync.Mutex

	// variables to store the backend configuration
	warehouses   []model.Warehouse
	warehousesMu sync.RWMutex

	connectionsMap   map[string]map[string]model.Warehouse // destID -> sourceID -> warehouse map
	connectionsMapMu sync.RWMutex

	sourceIDsByWorkspace   map[string][]string // workspaceID -> []sourceIDs
	sourceIDsByWorkspaceMu sync.RWMutex
}

func (bcm *BackendConfigManager) Start(ctx context.Context) {
	ch := bcm.tenantManager.WatchConfig(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-ch:
			if !ok {
				return
			}
			bcm.processData(ctx, data)
		}
	}
}

func (bcm *BackendConfigManager) Subscribe(ctx context.Context) <-chan []model.Warehouse {
	bcm.subscriptionsMu.Lock()
	defer bcm.subscriptionsMu.Unlock()

	ch := make(chan []model.Warehouse, 10)
	bcm.subscriptions = append(bcm.subscriptions, ch)

	bcm.warehousesMu.Lock()
	if len(bcm.warehouses) > 0 {
		ch <- bcm.warehouses
	}
	bcm.warehousesMu.Unlock()

	go func() {
		<-ctx.Done()

		bcm.subscriptionsMu.Lock()
		defer bcm.subscriptionsMu.Unlock()

		close(ch)

		for i, item := range bcm.subscriptions {
			if item == ch {
				bcm.subscriptions = append(bcm.subscriptions[:i], bcm.subscriptions[i+1:]...)
				return
			}
		}
	}()

	return ch
}

func (bcm *BackendConfigManager) processData(ctx context.Context, data map[string]backendconfig.ConfigT) {
	defer bcm.closeInitialConfigFetchedOnce.Do(func() {
		close(bcm.InitialConfigFetched)
	})

	var (
		warehouses           []model.Warehouse
		sourceIDsByWorkspace = make(map[string][]string)
		connectionsMap       = make(map[string]map[string]model.Warehouse)
	)

	for workspaceID, wConfig := range data {
		for _, source := range wConfig.Sources {
			if _, ok := sourceIDsByWorkspace[workspaceID]; !ok {
				sourceIDsByWorkspace[workspaceID] = make([]string, 0, len(wConfig.Sources))
			}
			sourceIDsByWorkspace[workspaceID] = append(sourceIDsByWorkspace[workspaceID], source.ID)

			for _, destination := range source.Destinations {
				if _, ok := whutils.WarehouseDestinationMap[destination.DestinationDefinition.Name]; !ok {
					bcm.logger.Debugf("Not a warehouse destination, skipping %s", destination.DestinationDefinition.Name)
					continue
				}

				if bcm.internalControlPlaneClient != nil {
					destination = bcm.attachSSHTunnellingInfo(ctx, destination)
				}

				warehouse := model.Warehouse{
					Source:      source,
					WorkspaceID: workspaceID,
					Destination: destination,
					Type:        destination.DestinationDefinition.Name,
					Namespace:   bcm.namespace(ctx, source, destination),
					Identifier:  whutils.GetWarehouseIdentifier(destination.DestinationDefinition.Name, source.ID, destination.ID),
				}

				warehouses = append(warehouses, warehouse)

				if _, ok := connectionsMap[destination.ID]; !ok {
					connectionsMap[destination.ID] = make(map[string]model.Warehouse)
				}
				connectionsMap[destination.ID][source.ID] = warehouse

				if destination.Config["sslMode"] == "verify-ca" {
					if err := whutils.WriteSSLKeys(destination); err.IsError() {
						bcm.logger.Error(err.Error())
						bcm.persistSSLFileErrorStat(
							workspaceID, destination.DestinationDefinition.Name, destination.Name, destination.ID,
							source.Name, source.ID, err.GetErrTag(),
						)
					}
				}
			}
		}
	}

	bcm.connectionsMapMu.Lock()
	bcm.connectionsMap = connectionsMap
	bcm.connectionsMapMu.Unlock()

	bcm.warehousesMu.Lock()
	bcm.warehouses = warehouses // TODO how is this used? because we are duplicating data
	bcm.warehousesMu.Unlock()

	bcm.sourceIDsByWorkspaceMu.Lock()
	bcm.sourceIDsByWorkspace = sourceIDsByWorkspace
	bcm.sourceIDsByWorkspaceMu.Unlock()

	bcm.subscriptionsMu.Lock()
	for _, sub := range bcm.subscriptions {
		sub <- warehouses
	}
	bcm.subscriptionsMu.Unlock()
}

// namespace gives the namespace for the warehouse in the following order
//  1. user set name from destinationConfig
//  2. from existing record in wh_schemas with same source + dest combo
//  3. convert source name
func (bcm *BackendConfigManager) namespace(ctx context.Context, source backendconfig.SourceT, destination backendconfig.DestinationT) string {
	destType := destination.DestinationDefinition.Name
	destConfig := destination.Config

	if destType == whutils.CLICKHOUSE {
		if database, ok := destConfig["database"].(string); ok {
			return database
		}
		return "rudder"
	}

	if destConfig["namespace"] != nil {
		namespace, _ := destConfig["namespace"].(string)
		if len(strings.TrimSpace(namespace)) > 0 {
			return whutils.ToProviderCase(destType, whutils.ToSafeNamespace(destType, namespace))
		}
	}

	namespacePrefix := bcm.conf.GetString(fmt.Sprintf("Warehouse.%s.customDatasetPrefix", whutils.WHDestNameMap[destType]), "")
	if namespacePrefix != "" {
		return whutils.ToProviderCase(destType, whutils.ToSafeNamespace(destType, fmt.Sprintf(`%s_%s`, namespacePrefix, source.Name)))
	}

	namespace, err := bcm.schema.GetNamespace(ctx, source.ID, destination.ID)
	if err != nil {
		bcm.logger.Errorw("getting namespace",
			logfield.SourceID, source.ID,
			logfield.DestinationID, destination.ID,
			logfield.DestinationType, destination.DestinationDefinition.Name,
			logfield.WorkspaceID, destination.WorkspaceID,
			logfield.Error, err.Error(),
		)
		return ""
	}
	if namespace == "" {
		return whutils.ToProviderCase(destType, whutils.ToSafeNamespace(destType, source.Name))
	}
	return namespace
}

func (bcm *BackendConfigManager) IsInitialized() bool {
	select {
	case <-bcm.InitialConfigFetched:
		return true
	default:
		return false
	}
}

func (bcm *BackendConfigManager) Connections() map[string]map[string]model.Warehouse {
	bcm.connectionsMapMu.RLock()
	defer bcm.connectionsMapMu.RUnlock()
	return bcm.connectionsMap
}

func (bcm *BackendConfigManager) ConnectionSourcesMap(destID string) (map[string]model.Warehouse, bool) {
	bcm.connectionsMapMu.RLock()
	defer bcm.connectionsMapMu.RUnlock()
	m, ok := bcm.connectionsMap[destID]
	return m, ok
}

func (bcm *BackendConfigManager) SourceIDsByWorkspace() map[string][]string {
	bcm.sourceIDsByWorkspaceMu.RLock()
	defer bcm.sourceIDsByWorkspaceMu.RUnlock()
	return bcm.sourceIDsByWorkspace
}

// WarehousesBySourceID gets all WHs for the given source ID
func (bcm *BackendConfigManager) WarehousesBySourceID(sourceID string) []model.Warehouse {
	bcm.warehousesMu.RLock()
	defer bcm.warehousesMu.RUnlock()

	return lo.Filter(bcm.warehouses, func(w model.Warehouse, _ int) bool {
		return w.Source.ID == sourceID
	})
}

// WarehousesByDestID gets all WHs for the given destination ID
func (bcm *BackendConfigManager) WarehousesByDestID(destID string) []model.Warehouse {
	bcm.warehousesMu.RLock()
	defer bcm.warehousesMu.RUnlock()

	return lo.Filter(bcm.warehouses, func(w model.Warehouse, _ int) bool {
		return w.Destination.ID == destID
	})
}

func (bcm *BackendConfigManager) attachSSHTunnellingInfo(
	ctx context.Context,
	upstream backendconfig.DestinationT,
) backendconfig.DestinationT {
	// at destination level, do we have tunnelling enabled.
	if tunnelEnabled := whutils.ReadAsBool("useSSH", upstream.Config); !tunnelEnabled {
		return upstream
	}

	bcm.logger.Debugf("Fetching ssh keys for destination: %s", upstream.ID)

	keys, err := bcm.internalControlPlaneClient.GetDestinationSSHKeys(ctx, upstream.ID)
	if err != nil {
		bcm.logger.Errorf("fetching ssh keys for destination: %s", err.Error())
		return upstream
	}

	replica := backendconfig.DestinationT{}
	if err := deepCopy(upstream, &replica); err != nil {
		bcm.logger.Errorf("deep copying the destination: %s failed: %s", upstream.ID, err)
		return upstream
	}

	replica.Config["sshPrivateKey"] = keys.PrivateKey
	return replica
}

func deepCopy(src, dest interface{}) error {
	buf, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return json.Unmarshal(buf, dest)
}

func (bcm *BackendConfigManager) persistSSLFileErrorStat(
	workspaceID, destType, destName,
	destID, sourceName, sourceID,
	errTag string,
) {
	tags := stats.Tags{
		"workspaceId":   workspaceID,
		"module":        "warehouse",
		"destType":      destType,
		"warehouseID":   misc.GetTagName(destID, sourceName, destName, misc.TailTruncateStr(sourceID, 6)),
		"sourceId":      sourceID,
		"destinationID": destID,
		"errTag":        errTag,
	}
	bcm.stats.NewTaggedStat("persist_ssl_file_failure", stats.CountType, tags).Count(1)
}

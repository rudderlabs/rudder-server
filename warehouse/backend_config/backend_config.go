package backend_config

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
		initialConfigFetched: make(chan struct{}),
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

	initialConfigFetched          chan struct{}
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

func (s *BackendConfigManager) InitialConfigFetched() chan struct{} {
	return s.initialConfigFetched
}

func (s *BackendConfigManager) Start(ctx context.Context) {
	ch := s.tenantManager.WatchConfig(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-ch:
			if !ok {
				return
			}
			s.processData(ctx, data)
		}
	}
}

func (s *BackendConfigManager) Subscribe(ctx context.Context) <-chan []model.Warehouse {
	s.subscriptionsMu.Lock()
	defer s.subscriptionsMu.Unlock()

	ch := make(chan []model.Warehouse, 10)
	s.subscriptions = append(s.subscriptions, ch)

	s.warehousesMu.Lock()
	if len(s.warehouses) > 0 {
		ch <- s.warehouses
	}
	s.warehousesMu.Unlock()

	go func() {
		<-ctx.Done()

		s.subscriptionsMu.Lock()
		defer s.subscriptionsMu.Unlock()

		close(ch)

		for i, item := range s.subscriptions {
			if item == ch {
				s.subscriptions = append(s.subscriptions[:i], s.subscriptions[i+1:]...)
				return
			}
		}
	}()

	return ch
}

func (s *BackendConfigManager) processData(ctx context.Context, data map[string]backendconfig.ConfigT) {
	defer s.closeInitialConfigFetchedOnce.Do(func() {
		close(s.initialConfigFetched)
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
					s.logger.Debugf("Not a warehouse destination, skipping %s", destination.DestinationDefinition.Name)
					continue
				}

				if s.internalControlPlaneClient != nil {
					destination = s.attachSSHTunnellingInfo(ctx, destination)
				}

				warehouse := model.Warehouse{
					Source:      source,
					WorkspaceID: workspaceID,
					Destination: destination,
					Type:        destination.DestinationDefinition.Name,
					Namespace:   s.namespace(ctx, source, destination),
					Identifier:  whutils.GetWarehouseIdentifier(destination.DestinationDefinition.Name, source.ID, destination.ID),
				}

				warehouses = append(warehouses, warehouse)

				if _, ok := connectionsMap[destination.ID]; !ok {
					connectionsMap[destination.ID] = make(map[string]model.Warehouse)
				}
				connectionsMap[destination.ID][source.ID] = warehouse

				if destination.Config["sslMode"] == "verify-ca" {
					if err := whutils.WriteSSLKeys(destination); err.IsError() {
						s.logger.Error(err.Error())
						persistSSLFileErrorStat(
							workspaceID, destination.DestinationDefinition.Name, destination.Name, destination.ID,
							source.Name, source.ID, err.GetErrTag(),
						)
					}
				}
			}
		}
	}

	s.connectionsMapMu.Lock()
	s.connectionsMap = connectionsMap
	s.connectionsMapMu.Unlock()

	s.warehousesMu.Lock()
	s.warehouses = warehouses // TODO how is this used? because we are duplicating data
	s.warehousesMu.Unlock()

	s.sourceIDsByWorkspaceMu.Lock()
	s.sourceIDsByWorkspace = sourceIDsByWorkspace
	s.sourceIDsByWorkspaceMu.Unlock()

	s.subscriptionsMu.Lock()
	for _, sub := range s.subscriptions {
		sub <- warehouses
	}
	s.subscriptionsMu.Unlock()
}

// namespace gives the namespace for the warehouse in the following order
//  1. user set name from destinationConfig
//  2. from existing record in wh_schemas with same source + dest combo
//  3. convert source name
func (s *BackendConfigManager) namespace(ctx context.Context, source backendconfig.SourceT, destination backendconfig.DestinationT) string {
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

	namespacePrefix := s.conf.GetString(fmt.Sprintf("Warehouse.%s.customDatasetPrefix", whutils.WHDestNameMap[destType]), "")
	if namespacePrefix != "" {
		return whutils.ToProviderCase(destType, whutils.ToSafeNamespace(destType, fmt.Sprintf(`%s_%s`, namespacePrefix, source.Name)))
	}

	namespace, err := s.schema.GetNamespace(ctx, source.ID, destination.ID)
	if err != nil {
		s.logger.Errorw("getting namespace",
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

func (s *BackendConfigManager) IsInitialized() bool {
	select {
	case <-s.initialConfigFetched:
		return true
	default:
		return false
	}
}

func (s *BackendConfigManager) Connections() map[string]map[string]model.Warehouse {
	s.connectionsMapMu.RLock()
	defer s.connectionsMapMu.RUnlock()
	return s.connectionsMap
}

func (s *BackendConfigManager) ConnectionSourcesMap(destID string) (map[string]model.Warehouse, bool) {
	s.connectionsMapMu.RLock()
	defer s.connectionsMapMu.RUnlock()
	m, ok := s.connectionsMap[destID]
	return m, ok
}

func (s *BackendConfigManager) SourceIDsByWorkspace() map[string][]string {
	s.sourceIDsByWorkspaceMu.RLock()
	defer s.sourceIDsByWorkspaceMu.RUnlock()
	return s.sourceIDsByWorkspace
}

// WarehousesBySourceID gets all WHs for the given source ID
func (s *BackendConfigManager) WarehousesBySourceID(sourceID string) []model.Warehouse {
	s.warehousesMu.RLock()
	defer s.warehousesMu.RUnlock()

	return lo.Filter(s.warehouses, func(w model.Warehouse, _ int) bool {
		return w.Source.ID == sourceID
	})
}

// WarehousesByDestID gets all WHs for the given destination ID
func (s *BackendConfigManager) WarehousesByDestID(destID string) []model.Warehouse {
	s.warehousesMu.RLock()
	defer s.warehousesMu.RUnlock()

	return lo.Filter(s.warehouses, func(w model.Warehouse, _ int) bool {
		return w.Destination.ID == destID
	})
}

func (s *BackendConfigManager) attachSSHTunnellingInfo(
	ctx context.Context,
	upstream backendconfig.DestinationT,
) backendconfig.DestinationT {
	// at destination level, do we have tunnelling enabled.
	if tunnelEnabled := whutils.ReadAsBool("useSSH", upstream.Config); !tunnelEnabled {
		return upstream
	}

	s.logger.Debugf("Fetching ssh keys for destination: %s", upstream.ID)

	keys, err := s.internalControlPlaneClient.GetDestinationSSHKeys(ctx, upstream.ID)
	if err != nil {
		s.logger.Errorf("fetching ssh keys for destination: %s", err.Error())
		return upstream
	}

	replica := backendconfig.DestinationT{}
	if err := deepCopy(upstream, &replica); err != nil {
		s.logger.Errorf("deep copying the destination: %s failed: %s", upstream.ID, err)
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

func persistSSLFileErrorStat(workspaceID, destType, destName, destID, sourceName, sourceID, errTag string) {
	tags := stats.Tags{
		"workspaceId":   workspaceID,
		"module":        "warehouse",
		"destType":      destType,
		"warehouseID":   misc.GetTagName(destID, sourceName, destName, misc.TailTruncateStr(sourceID, 6)),
		"destinationID": destID,
		"errTag":        errTag,
	}
	stats.Default.NewTaggedStat("persist_ssl_file_failure", stats.CountType, tags).Count(1)
}

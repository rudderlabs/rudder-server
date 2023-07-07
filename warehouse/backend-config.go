package warehouse

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"

	"golang.org/x/exp/slices"

	"github.com/rudderlabs/rudder-go-kit/config"
	bc "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// TODO: add tests
// TODO: add graceful shutdown
// TODO: add a ready mechanism to the getters

// backendConfigManager is used to handle the backend configuration in the Warehouse
type backendConfigManager struct {
	conf            *config.Config
	db              *sql.DB
	schema          *repo.WHSchema
	backendConfig   bc.BackendConfig
	enableTunneling bool

	started              atomic.Bool
	initialConfigFetched atomic.Bool
	subscriptions        []chan model.Warehouse
	subscriptionsMu      sync.Mutex

	// variables to store the backend configuration
	warehouses   []model.Warehouse
	warehousesMu sync.RWMutex

	connectionsMap   map[string]map[string]model.Warehouse // destID -> sourceID -> warehouse map
	connectionsMapMu sync.RWMutex

	sourceIDsByWorkspace   map[string][]string // workspaceID -> []sourceIDs
	sourceIDsByWorkspaceMu sync.RWMutex

	workspaceBySourceIDs   map[string]string // sourceID -> workspaceID
	workspaceBySourceIDsMu sync.RWMutex
}

func newBackendConfigManager(
	c *config.Config, // TODO possibly use this to get all the needed variables
	db *sql.DB,
	wrappedDB *sqlquerywrapper.DB,
	backendConfig bc.BackendConfig,
	enableTunneling bool,
) *backendConfigManager {
	return &backendConfigManager{
		conf:            c,
		db:              db,
		schema:          repo.NewWHSchemas(wrappedDB),
		backendConfig:   backendConfig,
		enableTunneling: enableTunneling,

		connectionsMap:       make(map[string]map[string]model.Warehouse),
		workspaceBySourceIDs: make(map[string]string),
	}
}

func (s *backendConfigManager) Start(ctx context.Context) {
	if !s.started.CompareAndSwap(false, true) {
		return // already started
	}

	ch := s.backendConfig.Subscribe(ctx, bc.TopicBackendConfig)
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-ch:
			s.processData(ctx, data.Data.(map[string]bc.ConfigT))
		}
	}
}

func (s *backendConfigManager) Subscribe(ctx context.Context) <-chan model.Warehouse {
	s.subscriptionsMu.Lock()
	defer s.subscriptionsMu.Unlock()

	ch := make(chan model.Warehouse, 10)
	s.subscriptions = append(s.subscriptions, ch)

	return ch // TODO ctx
}

func (s *backendConfigManager) processData(ctx context.Context, data map[string]bc.ConfigT) {
	defer s.initialConfigFetched.Store(true)

	var (
		warehouses               []model.Warehouse
		connectionFlags          bc.ConnectionFlags
		sourceIDsByWorkspaceTemp = make(map[string][]string)
	)
	for workspaceID, wConfig := range data {
		// the last connection flags should be enough, since they are all the same in multi-workspace environments
		connectionFlags = wConfig.ConnectionFlags
		// map source IDs to workspace IDs
		workspaceBySourceIDs := make(map[string]string)

		for _, source := range wConfig.Sources {
			workspaceBySourceIDs[source.ID] = workspaceID

			if _, ok := sourceIDsByWorkspaceTemp[workspaceID]; !ok {
				sourceIDsByWorkspaceTemp[workspaceID] = make([]string, 0, len(wConfig.Sources))
			}
			sourceIDsByWorkspaceTemp[workspaceID] = append(sourceIDsByWorkspaceTemp[workspaceID], source.ID)

			for _, destination := range source.Destinations {
				if !slices.Contains(whutils.WarehouseDestinations, destination.DestinationDefinition.Name) {
					continue
				}

				wh := &HandleT{
					dbHandle:     s.db, // TODO replace with *sqlquerywrapper.DB?
					whSchemaRepo: s.schema,
					conf:         s.conf,
					destType:     destination.DestinationDefinition.Name,
				}
				if s.enableTunneling {
					destination = wh.attachSSHTunnellingInfo(ctx, destination)
				}

				warehouse := model.Warehouse{
					Source:      source,
					WorkspaceID: workspaceID,
					Destination: destination,
					Type:        wh.destType,
					Namespace:   wh.getNamespace(ctx, source, destination),
					Identifier:  whutils.GetWarehouseIdentifier(wh.destType, source.ID, destination.ID),
				}

				warehouses = append(warehouses, warehouse)

				s.subscriptionsMu.Lock()
				for _, sub := range s.subscriptions {
					sub <- warehouse
				}
				s.subscriptionsMu.Unlock()

				s.connectionsMapMu.Lock()
				if _, ok := s.connectionsMap[destination.ID]; !ok {
					s.connectionsMap[destination.ID] = make(map[string]model.Warehouse)
				}
				s.connectionsMap[destination.ID][source.ID] = warehouse
				s.connectionsMapMu.Unlock()

				if destination.Config["sslMode"] == "verify-ca" {
					if err := whutils.WriteSSLKeys(destination); err.IsError() {
						wh.Logger.Error(err.Error())
						persistSSLFileErrorStat(
							workspaceID, wh.destType, destination.Name, destination.ID,
							source.Name, source.ID, err.GetErrTag(),
						)
					}
				}

				if whutils.IDResolutionEnabled() && slices.Contains(whutils.IdentityEnabledWarehouses, warehouse.Type) {
					wh.setupIdentityTables(ctx, warehouse)
					if shouldPopulateHistoricIdentities && warehouse.Destination.Enabled {
						// non-blocking populate historic identities
						wh.populateHistoricIdentities(ctx, warehouse)
					}
				}
			}
		}
	}

	s.warehousesMu.Lock()
	s.warehouses = warehouses // TODO how is this used? because we are duplicating data
	s.warehousesMu.Unlock()
	s.sourceIDsByWorkspaceMu.Lock()
	s.sourceIDsByWorkspace = sourceIDsByWorkspaceTemp
	s.sourceIDsByWorkspaceMu.Unlock()

	if val, ok := connectionFlags.Services["warehouse"]; ok {
		if UploadAPI.connectionManager != nil {
			UploadAPI.connectionManager.Apply(connectionFlags.URL, val)
		}
	}
}

func (s *backendConfigManager) IsInitialized() bool {
	return s.initialConfigFetched.Load()
}

func (s *backendConfigManager) Connections() map[string]map[string]model.Warehouse {
	s.connectionsMapMu.RLock()
	defer s.connectionsMapMu.RUnlock()
	return s.connectionsMap
}

func (s *backendConfigManager) ConnectionSourcesMap(destID string) (map[string]model.Warehouse, bool) {
	s.connectionsMapMu.RLock()
	defer s.connectionsMapMu.RUnlock()
	m, ok := s.connectionsMap[destID]
	return m, ok
}

func (s *backendConfigManager) SourceIDsByWorkspace() map[string][]string {
	s.sourceIDsByWorkspaceMu.RLock()
	defer s.sourceIDsByWorkspaceMu.RUnlock()
	return s.sourceIDsByWorkspace
}

// WarehousesBySourceID gets all WHs for the given source ID
func (s *backendConfigManager) WarehousesBySourceID(sourceID string) []model.Warehouse {
	s.warehousesMu.RLock()
	defer s.warehousesMu.RUnlock()
	var warehouses []model.Warehouse
	for _, wh := range s.warehouses {
		if wh.Source.ID == sourceID {
			warehouses = append(warehouses, wh)
		}
	}
	return warehouses
}

// WarehousesByDestID gets all WHs for the given destination ID
func (s *backendConfigManager) WarehousesByDestID(destID string) []model.Warehouse {
	s.warehousesMu.RLock()
	defer s.warehousesMu.RUnlock()
	var warehouses []model.Warehouse
	for _, wh := range s.warehouses {
		if wh.Destination.ID == destID {
			warehouses = append(warehouses, wh)
		}
	}
	return warehouses
}

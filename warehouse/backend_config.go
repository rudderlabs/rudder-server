package warehouse

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/samber/lo"

	"golang.org/x/exp/slices"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	cpclient "github.com/rudderlabs/rudder-server/warehouse/client/controlplane"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func newBackendConfigManager(
	c *config.Config,
	db *sqlquerywrapper.DB,
	bc backendconfig.BackendConfig,
	log logger.Logger,
) *backendConfigManager {
	if c == nil {
		c = config.Default
	}
	if bc == nil {
		bc = backendconfig.DefaultBackendConfig
	}
	if log == nil {
		log = logger.NOP
	}
	bcm := &backendConfigManager{
		conf:                 c,
		db:                   db,
		schema:               repo.NewWHSchemas(db),
		backendConfig:        bc,
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
	bcm.shouldPopulateHistoricIdentities = c.GetBool("Warehouse.populateHistoricIdentities", false)
	return bcm
}

// backendConfigManager is used to handle the backend configuration in the Warehouse
type backendConfigManager struct {
	conf                       *config.Config
	db                         *sqlquerywrapper.DB
	schema                     *repo.WHSchema
	backendConfig              backendconfig.BackendConfig
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

	shouldPopulateHistoricIdentities bool
}

func (s *backendConfigManager) Start(ctx context.Context) {
	ch := s.backendConfig.Subscribe(ctx, backendconfig.TopicBackendConfig)
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-ch:
			if !ok {
				return
			}
			s.processData(ctx, data.Data.(map[string]backendconfig.ConfigT))
		}
	}
}

func (s *backendConfigManager) Subscribe(ctx context.Context) <-chan []model.Warehouse {
	s.subscriptionsMu.Lock()
	defer s.subscriptionsMu.Unlock()

	idx := len(s.subscriptions)
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

		s.subscriptions = append(s.subscriptions[:idx], s.subscriptions[idx+1:]...)
	}()

	return ch
}

func (s *backendConfigManager) processData(ctx context.Context, data map[string]backendconfig.ConfigT) {
	defer s.closeInitialConfigFetchedOnce.Do(func() {
		close(s.initialConfigFetched)
	})

	var (
		warehouses           []model.Warehouse
		connectionFlags      backendconfig.ConnectionFlags
		sourceIDsByWorkspace = make(map[string][]string)
	)

	for workspaceID, wConfig := range data {
		// the last connection flags should be enough, since they are all the same in multi-workspace environments
		connectionFlags = wConfig.ConnectionFlags

		for _, source := range wConfig.Sources {
			if _, ok := sourceIDsByWorkspace[workspaceID]; !ok {
				sourceIDsByWorkspace[workspaceID] = make([]string, 0, len(wConfig.Sources))
			}
			sourceIDsByWorkspace[workspaceID] = append(sourceIDsByWorkspace[workspaceID], source.ID)

			for _, destination := range source.Destinations {
				if _, ok := warehouseutils.WarehouseDestinationMap[destination.DestinationDefinition.Name]; !ok {
					s.logger.Debugf("Not a warehouse destination, skipping %s", destination.DestinationDefinition.Name)
					continue
				}

				wh := &Router{
					dbHandle:     s.db,
					whSchemaRepo: s.schema,
					conf:         s.conf,
					destType:     destination.DestinationDefinition.Name,
				}
				if s.internalControlPlaneClient != nil {
					destination = s.attachSSHTunnellingInfo(ctx, destination)
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

				s.connectionsMapMu.Lock()
				if _, ok := s.connectionsMap[destination.ID]; !ok {
					s.connectionsMap[destination.ID] = make(map[string]model.Warehouse)
				}
				s.connectionsMap[destination.ID][source.ID] = warehouse
				s.connectionsMapMu.Unlock()

				if destination.Config["sslMode"] == "verify-ca" {
					if err := whutils.WriteSSLKeys(destination); err.IsError() {
						s.logger.Error(err.Error())
						persistSSLFileErrorStat(
							workspaceID, wh.destType, destination.Name, destination.ID,
							source.Name, source.ID, err.GetErrTag(),
						)
					}
				}

				if whutils.IDResolutionEnabled() && slices.Contains(whutils.IdentityEnabledWarehouses, warehouse.Type) {
					wh.setupIdentityTables(ctx, warehouse)
					if s.shouldPopulateHistoricIdentities && warehouse.Destination.Enabled {
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
	s.sourceIDsByWorkspace = sourceIDsByWorkspace
	s.sourceIDsByWorkspaceMu.Unlock()

	s.subscriptionsMu.Lock()
	for _, sub := range s.subscriptions {
		sub <- warehouses
	}
	s.subscriptionsMu.Unlock()

	if val, ok := connectionFlags.Services["warehouse"]; ok {
		if UploadAPI.connectionManager != nil {
			UploadAPI.connectionManager.Apply(connectionFlags.URL, val)
		}
	}
}

func (s *backendConfigManager) IsInitialized() bool {
	select {
	case <-s.initialConfigFetched:
		return true
	default:
		return false
	}
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

	return lo.Filter(s.warehouses, func(w model.Warehouse, _ int) bool {
		return w.Source.ID == sourceID
	})
}

// WarehousesByDestID gets all WHs for the given destination ID
func (s *backendConfigManager) WarehousesByDestID(destID string) []model.Warehouse {
	s.warehousesMu.RLock()
	defer s.warehousesMu.RUnlock()

	return lo.Filter(s.warehouses, func(w model.Warehouse, _ int) bool {
		return w.Destination.ID == destID
	})
}

func (s *backendConfigManager) attachSSHTunnellingInfo(
	ctx context.Context,
	upstream backendconfig.DestinationT,
) backendconfig.DestinationT {
	// at destination level, do we have tunnelling enabled.
	if tunnelEnabled := warehouseutils.ReadAsBool("useSSH", upstream.Config); !tunnelEnabled {
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

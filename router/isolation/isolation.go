package isolation

import (
	"context"
	"errors"
	"sync"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	throttlerconfig "github.com/rudderlabs/rudder-server/router/throttler/config"
	"github.com/rudderlabs/rudder-server/router/types"
)

type Mode string

const (
	ModeNone        Mode = "none"
	ModeWorkspace   Mode = "workspace"
	ModeDestination Mode = "destination"
)

// GetStrategy returns the strategy for the given isolation mode. An error is returned if the mode is invalid
func GetStrategy(mode Mode, destType string, partitionFilter func(destinationID string) bool, c *config.Config) (Strategy, error) {
	switch mode {
	case ModeNone:
		return noneStrategy{}, nil
	case ModeWorkspace:
		return workspaceStrategy{customVal: destType}, nil
	case ModeDestination:
		return &destinationStrategy{
			config: c,
			pickupQueryThrottlingEnabled: c.GetReloadableBoolVar(false,
				"Router.throttler."+destType+".pickupQueryThrottlingEnabled",
				"Router.throttler.pickupQueryThrottlingEnabled",
				// TODO: remove the below deprecated config keys in the next release
				"Router."+destType+".pickupQueryThrottlingEnabled",
				"Router.pickupQueryThrottlingEnabled",
			),
			destinationFilter:           partitionFilter,
			destType:                    destType,
			throttlerPerEventTypeConfig: make(map[string]config.ValueLoader[bool]),
		}, nil
	default:
		return noneStrategy{}, errors.New("unsupported isolation mode")
	}
}

// Strategy defines the operations that every different isolation strategy in processor must implement
type Strategy interface {
	// ActivePartitions returns the list of partitions that are active for the given strategy
	ActivePartitions(ctx context.Context, db jobsdb.JobsDB) ([]string, error)
	// AugmentQueryParams augments the given GetQueryParamsT with the strategy specific parameters
	AugmentQueryParams(partition string, params *jobsdb.GetQueryParams)
	// StopIteration returns true if the iteration should be stopped for the given error
	StopIteration(err error, destinationID string) bool
	// StopQueries returns true if the iterator should stop fetching more jobs from jobsDB
	StopQueries(err error, destinationID string) bool
	// SupportsPickupQueryThrottling returns true if the strategy supports pickup query throttling, i.e., if it can throttle queries to jobsDB based on the throttling limits set at destination level
	SupportsPickupQueryThrottling() bool
}

// noneStrategy implements isolation at no level
type noneStrategy struct{}

func (noneStrategy) ActivePartitions(_ context.Context, _ jobsdb.JobsDB) ([]string, error) {
	return []string{""}, nil
}

func (noneStrategy) AugmentQueryParams(_ string, _ *jobsdb.GetQueryParams) {
	// no-op
}

func (noneStrategy) StopIteration(_ error, _ string) bool {
	return false
}

func (noneStrategy) StopQueries(_ error, _ string) bool {
	return false
}

func (noneStrategy) SupportsPickupQueryThrottling() bool {
	return false
}

// workspaceStrategy implements isolation at workspace level
type workspaceStrategy struct {
	customVal string
}

// ActivePartitions returns the list of active workspaceIDs in jobsdb
func (ws workspaceStrategy) ActivePartitions(ctx context.Context, db jobsdb.JobsDB) ([]string, error) {
	return db.GetDistinctParameterValues(ctx, jobsdb.WorkspaceID, ws.customVal)
}

func (workspaceStrategy) AugmentQueryParams(partition string, params *jobsdb.GetQueryParams) {
	params.WorkspaceID = partition
}

func (workspaceStrategy) StopIteration(_ error, _ string) bool {
	return false
}

func (workspaceStrategy) StopQueries(_ error, _ string) bool {
	return false
}

func (workspaceStrategy) SupportsPickupQueryThrottling() bool {
	return false
}

// destinationStrategy implements isolation at destination level
type destinationStrategy struct {
	config                        *config.Config
	pickupQueryThrottlingEnabled  config.ValueLoader[bool]
	destinationFilter             func(destinationID string) bool
	destType                      string
	throttlerPerEventTypeConfigMu sync.RWMutex
	throttlerPerEventTypeConfig   map[string]config.ValueLoader[bool]
}

// ActivePartitions returns the list of active destinationIDs in jobsdb
func (ds *destinationStrategy) ActivePartitions(ctx context.Context, db jobsdb.JobsDB) ([]string, error) {
	unfiltered, err := db.GetDistinctParameterValues(ctx, jobsdb.DestinationID, "")
	if err != nil {
		return nil, err
	}
	return lo.Filter(unfiltered, func(destinationID string, _ int) bool {
		return ds.destinationFilter(destinationID)
	}), nil
}

// AugmentQueryParams augments the given GetQueryParamsT by adding the partition as sourceID parameter filter
func (*destinationStrategy) AugmentQueryParams(partition string, params *jobsdb.GetQueryParams) {
	params.ParameterFilters = append(params.ParameterFilters, jobsdb.ParameterFilterT{Name: "destination_id", Value: partition})
}

// StopIteration returns true if the error is ErrDestinationThrottled
func (ds *destinationStrategy) StopIteration(err error, destinationID string) bool {
	return errors.Is(err, types.ErrDestinationThrottled) && !ds.hasDestinationThrottlerPerEventType(destinationID)
}

// StopQueries returns true if the error is ErrDestinationThrottled and throttlerPerEventType is enabled for the destination
func (ds *destinationStrategy) StopQueries(err error, destinationID string) bool {
	return errors.Is(err, types.ErrDestinationThrottled) && ds.hasDestinationThrottlerPerEventType(destinationID)
}

func (ds *destinationStrategy) SupportsPickupQueryThrottling() bool {
	return ds.pickupQueryThrottlingEnabled.Load()
}

func (ds *destinationStrategy) hasDestinationThrottlerPerEventType(destinationID string) bool {
	ds.throttlerPerEventTypeConfigMu.RLock()
	throttlerPerEventTypeConfig, ok := ds.throttlerPerEventTypeConfig[destinationID]
	ds.throttlerPerEventTypeConfigMu.RUnlock()
	if !ok {
		ds.throttlerPerEventTypeConfigMu.Lock()
		throttlerPerEventTypeConfig, ok = ds.throttlerPerEventTypeConfig[destinationID]
		if !ok {
			throttlerPerEventTypeConfig = throttlerconfig.ThrottlerPerEventTypeEnabled(ds.config, ds.destType, destinationID)
			ds.throttlerPerEventTypeConfig[destinationID] = throttlerPerEventTypeConfig
		}
		ds.throttlerPerEventTypeConfigMu.Unlock()
	}
	return throttlerPerEventTypeConfig.Load()
}

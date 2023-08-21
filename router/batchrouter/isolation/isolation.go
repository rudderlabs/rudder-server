package isolation

import (
	"context"
	"errors"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

type Mode string

const (
	ModeNone        Mode = "none"
	ModeWorkspace   Mode = "workspace"
	ModeDestination Mode = "destination"
)

// GetStrategy returns the strategy for the given isolation mode. An error is returned if the mode is invalid
func GetStrategy(mode Mode, customVal string, destinationFilter func(destinationID string) bool) (Strategy, error) {
	switch mode {
	case ModeNone:
		return noneStrategy{}, nil
	case ModeWorkspace:
		return workspaceStrategy{customVal: customVal}, nil
	case ModeDestination:
		return destinationStrategy{destinationFilter: destinationFilter}, nil
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
}

// noneStrategy implements isolation at no level
type noneStrategy struct{}

func (noneStrategy) ActivePartitions(_ context.Context, _ jobsdb.JobsDB) ([]string, error) {
	return []string{""}, nil
}

func (noneStrategy) AugmentQueryParams(_ string, _ *jobsdb.GetQueryParams) {
	// no-op
}

// workspaceStrategy implements isolation at workspace level
type workspaceStrategy struct {
	customVal string
}

// ActivePartitions returns the list of active workspaceIDs in jobsdb
func (ws workspaceStrategy) ActivePartitions(ctx context.Context, db jobsdb.JobsDB) ([]string, error) {
	return db.GetActiveWorkspaces(ctx, ws.customVal)
}

func (workspaceStrategy) AugmentQueryParams(partition string, params *jobsdb.GetQueryParams) {
	params.WorkspaceID = partition
}

// destinationStrategy implements isolation at destination level
type destinationStrategy struct {
	destinationFilter func(destinationID string) bool
}

// ActivePartitions returns the list of active destinationIDs in jobsdb
func (ds destinationStrategy) ActivePartitions(ctx context.Context, db jobsdb.JobsDB) ([]string, error) {
	unfiltered, err := db.GetDistinctParameterValues(ctx, "destination_id")
	if err != nil {
		return nil, err
	}
	return lo.Filter(unfiltered, func(destinationID string, _ int) bool {
		return ds.destinationFilter(destinationID)
	}), nil
}

// AugmentQueryParams augments the given GetQueryParamsT by adding the partition as sourceID parameter filter
func (destinationStrategy) AugmentQueryParams(partition string, params *jobsdb.GetQueryParams) {
	params.ParameterFilters = append(params.ParameterFilters, jobsdb.ParameterFilterT{Name: "destination_id", Value: partition})
}

package isolation

import (
	"context"
	"errors"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

type Mode string

const (
	ModeNone      Mode = "none"
	ModeWorkspace Mode = "workspace"
)

// GetStrategy returns the strategy for the given isolation mode. An error is returned if the mode is invalid
func GetStrategy(mode Mode) (Strategy, error) {
	switch mode {
	case ModeNone:
		return noneStrategy{}, nil
	case ModeWorkspace:
		return workspaceStrategy{}, nil
	default:
		return noneStrategy{}, errors.New("unsupported isolation mode")
	}
}

// Strategy defines the operations that every different isolation strategy in processor must implement
type Strategy interface {
	// ActivePartitions returns the list of partitions that are active for the given strategy
	ActivePartitions(ctx context.Context, db jobsdb.JobsDB) ([]string, error)
	// AugmentQueryParams augments the given GetQueryParamsT with the strategy specific parameters
	AugmentQueryParams(partition string, params *jobsdb.GetQueryParamsT)
}

// noneStrategy implements isolation at no level
type noneStrategy struct{}

func (noneStrategy) ActivePartitions(_ context.Context, _ jobsdb.JobsDB) ([]string, error) {
	return []string{""}, nil
}

func (noneStrategy) AugmentQueryParams(_ string, _ *jobsdb.GetQueryParamsT) {
	// no-op
}

// workspaceStrategy implements isolation at workspace level
type workspaceStrategy struct{}

// ActivePartitions returns the list of active workspaceIDs in jobsdb
func (workspaceStrategy) ActivePartitions(ctx context.Context, db jobsdb.JobsDB) ([]string, error) {
	return db.GetActiveWorkspaces(ctx)
}

func (workspaceStrategy) AugmentQueryParams(partition string, params *jobsdb.GetQueryParamsT) {
	params.WorkspaceID = partition
}

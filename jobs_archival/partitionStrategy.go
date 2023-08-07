package archiver

import (
	"context"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

const (
	SourcePartition      = "source"
	DestinationPartition = "destination"
	WorkspacePartition   = "workspace"
)

type partitionStrategy interface {
	activePartitions(ctx context.Context, db jobsdb.JobsDB) ([]string, error)
	augmentQueryParams(partition string, params *jobsdb.GetQueryParamsT)
}

func getPartitionStrategy(partitionType string) partitionStrategy {
	switch partitionType {
	case SourcePartition:
		return sourceStrategy{}
	case DestinationPartition:
		return destinationStrategy{}
	case WorkspacePartition:
		return workspaceStrategy{}
	default:
		panic("invalid partition type")
	}
}

// sourceStrategy implements isolation at source level
type sourceStrategy struct{}

// activePartitions returns the list of active sourceIDs in jobsdb
func (sourceStrategy) activePartitions(ctx context.Context, db jobsdb.JobsDB) ([]string, error) {
	return db.GetDistinctParameterValues(ctx, "source_id")
}

// augmentQueryParams augments the given GetQueryParamsT by adding the partition as sourceID parameter filter
func (sourceStrategy) augmentQueryParams(partition string, params *jobsdb.GetQueryParamsT) {
	params.ParameterFilters = append(
		params.ParameterFilters,
		jobsdb.ParameterFilterT{Name: "source_id", Value: partition},
	)
}

type destinationStrategy struct{}

func (destinationStrategy) activePartitions(ctx context.Context, db jobsdb.JobsDB) ([]string, error) {
	return db.GetDistinctParameterValues(ctx, "destination_id")
}

func (destinationStrategy) augmentQueryParams(partition string, params *jobsdb.GetQueryParamsT) {
	params.ParameterFilters = append(
		params.ParameterFilters,
		jobsdb.ParameterFilterT{Name: "destination_id", Value: partition},
	)
}

type workspaceStrategy struct{}

func (workspaceStrategy) activePartitions(ctx context.Context, db jobsdb.JobsDB) ([]string, error) {
	return db.GetActiveWorkspaces(ctx, "")
}

func (workspaceStrategy) augmentQueryParams(partition string, params *jobsdb.GetQueryParamsT) {
	params.WorkspaceID = partition
}

package targetnode

import (
	"context"

	"golang.org/x/sync/errgroup"

	etcdtypes "github.com/rudderlabs/rudder-schemas/go/cluster"
)

// Migrator defines the interface for a target node migrator
type Migrator interface {
	// Handle prepares the target node for starting a new partition migration
	Handle(ctx context.Context, migration *etcdtypes.PartitionMigration) error

	// Run starts a gRPC server for accepting jobs from source nodes and watches for moved migration jobs assigned to this target node
	Run(ctx context.Context, wg *errgroup.Group) error
}

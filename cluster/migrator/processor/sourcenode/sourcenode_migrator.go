package sourcenode

import (
	"context"

	"golang.org/x/sync/errgroup"

	etcdtypes "github.com/rudderlabs/rudder-schemas/go/cluster"
)

// Migrator defines the interface for a source node migrator
type Migrator interface {
	// Handle prepares the source node for starting a new partition migration
	Handle(ctx context.Context, migration *etcdtypes.PartitionMigration) error

	// Run watches for new migration jobs assigned to this source node
	Run(ctx context.Context, wg *errgroup.Group) error
}

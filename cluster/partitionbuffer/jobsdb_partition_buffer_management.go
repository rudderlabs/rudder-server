package partitionbuffer

import (
	"context"
	"fmt"
	"slices"

	"github.com/lib/pq"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/utils/tx"
)

// BufferPartitions marks the provided partition ids to be buffered
func (b *jobsDBPartitionBuffer) BufferPartitions(ctx context.Context, partitionIds []string) error {
	var newVersion int
	// dedup and sort partitionIds to avoid deadlocks
	partitionIds = lo.Uniq(partitionIds)
	slices.Sort(partitionIds)
	if err := b.WithTx(func(tx *tx.Tx) error {
		query := `INSERT INTO ` + b.Identifier() + `_buffered_partitions (partition_id) VALUES ($1) ON CONFLICT DO NOTHING`
		for _, partition := range partitionIds {
			if _, err := tx.ExecContext(ctx, query, partition); err != nil {
				return fmt.Errorf("adding buffered partition %s: %w", partition, err)
			}
		}
		var err error
		newVersion, err = b.getBufferedPartitionsVersionInTx(ctx, tx)
		return err
	}); err != nil {
		return fmt.Errorf("setting buffered partitions: %w", err)
	}
	b.bufferedPartitionsMu.Lock()
	defer b.bufferedPartitionsMu.Unlock()
	b.bufferedPartitions = b.bufferedPartitions.Append(lo.SliceToMap(partitionIds, func(partition string) (string, struct{}) {
		return partition, struct{}{}
	}))
	b.bufferedPartitionsVersion = newVersion
	return nil
}

func (b *jobsDBPartitionBuffer) RefreshBufferedPartitions(ctx context.Context) error {
	var (
		dbVersion          int
		bufferedPartitions *readOnlyMap[string, struct{}]
	)
	err := b.WithTx(func(tx *tx.Tx) (err error) {
		dbVersion, err = b.getBufferedPartitionsVersionInTx(ctx, tx)
		if err != nil {
			return err
		}
		bufferedPartitions, err = b.getBufferedPartitionsInTx(ctx, tx)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("refreshing buffered partitions: %w", err)
	}
	b.bufferedPartitionsMu.Lock()
	defer b.bufferedPartitionsMu.Unlock()
	b.bufferedPartitionsVersion = dbVersion
	b.bufferedPartitions = bufferedPartitions
	return nil
}

// getBufferedPartitionsInTx fetches the list of buffered partitions from the database within the provided transaction
func (b *jobsDBPartitionBuffer) getBufferedPartitionsInTx(ctx context.Context, tx *tx.Tx) (*readOnlyMap[string, struct{}], error) {
	bufferedPartitions := make(map[string]struct{})
	query := `SELECT partition_id FROM ` + b.Identifier() + `_buffered_partitions`
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying buffered partitions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var partitionID string
		if err := rows.Scan(&partitionID); err != nil {
			return nil, fmt.Errorf("scanning buffered partition ID: %w", err)
		}
		bufferedPartitions[partitionID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating buffered partitions rows: %w", err)
	}
	return newReadOnlyMap(bufferedPartitions), nil
}

// getBufferedPartitionsVersionInTx fetches the version of buffered partitions from the database within the provided transaction
func (b *jobsDBPartitionBuffer) getBufferedPartitionsVersionInTx(ctx context.Context, tx *tx.Tx) (int, error) {
	var dbVersion int
	query := `SELECT version FROM buffered_partitions_versions where key = ` + pq.QuoteLiteral(b.Identifier()) + ` FOR SHARE` // FOR SHARE used as a read lock
	if err := tx.QueryRowContext(ctx, query).Scan(&dbVersion); err != nil {
		return 0, fmt.Errorf("querying buffered partitions version: %w", err)
	}
	return dbVersion, nil
}

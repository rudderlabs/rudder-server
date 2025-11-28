package jobsdb

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/lib/pq"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/utils/tx"
)

// Management interface for read excluded partitions
type ReadExcludedPartitionsManager interface {
	// AddReadExcludedPartitionIDs adds partition IDs to the excluded read list
	AddReadExcludedPartitionIDs(ctx context.Context, partitionIDs []string) error
	// RemoveReadExcludedPartitionIDs removes partition IDs from the excluded read list
	RemoveReadExcludedPartitionIDs(ctx context.Context, partitionIDs []string) error
}

func (jd *Handle) AddReadExcludedPartitionIDs(ctx context.Context, partitionIDs []string) error {
	if jd.conf.numPartitions <= 0 {
		return fmt.Errorf("partitioning is not enabled for prefix %s", jd.tablePrefix)
	}
	if len(partitionIDs) == 0 {
		return nil
	}

	partitionIDs = lo.Uniq(partitionIDs)
	slices.Sort(partitionIDs) // Sort to avoid deadlocks
	if err := jd.WithTx(func(tx *tx.Tx) error {
		query := `INSERT INTO ` + jd.tablePrefix + `_read_excluded_partitions (partition_id) VALUES ($1) ON CONFLICT DO NOTHING`
		for _, partitionID := range partitionIDs {
			if _, err := tx.ExecContext(ctx, query, partitionID); err != nil {
				return fmt.Errorf("adding excluded read partition ID %s: %w", partitionID, err)
			}
		}
		jd.noResultsCache.InvalidatePartitions(partitionIDs)
		return nil
	}); err != nil {
		return err
	}

	jd.excludedReadPartitionsLock.Lock()
	defer jd.excludedReadPartitionsLock.Unlock()
	if jd.excludedReadPartitions == nil {
		jd.excludedReadPartitions = make(map[string]struct{})
	}
	for _, partitionID := range partitionIDs {
		jd.excludedReadPartitions[partitionID] = struct{}{}
	}

	return nil
}

func (jd *Handle) RemoveReadExcludedPartitionIDs(ctx context.Context, partitionIDs []string) error {
	if jd.conf.numPartitions <= 0 {
		return fmt.Errorf("partitioning is not enabled for prefix %s", jd.tablePrefix)
	}
	if len(partitionIDs) == 0 {
		return nil
	}

	partitionIDs = lo.Uniq(partitionIDs)
	slices.Sort(partitionIDs) // Sort to avoid deadlocks
	query := `DELETE FROM ` + jd.tablePrefix + `_read_excluded_partitions WHERE partition_id IN (` +
		strings.Join(lo.Map(partitionIDs, func(p string, _ int) string { return pq.QuoteLiteral(p) }), ",") + `)`

	if _, err := jd.dbHandle.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("removing excluded read partition IDs %+v: %w", partitionIDs, err)
	}
	jd.excludedReadPartitionsLock.Lock()
	defer jd.excludedReadPartitionsLock.Unlock()
	for _, partitionID := range partitionIDs {
		delete(jd.excludedReadPartitions, partitionID)
	}
	jd.noResultsCache.InvalidatePartitions(partitionIDs)
	return nil
}

// loadReadExcludedPartitions loads the excluded read partitions from the database
// and populates the excludedReadPartitions map in the JobsDB handle.
func (jd *Handle) loadReadExcludedPartitions() error {
	jd.excludedReadPartitionsLock.Lock()
	defer jd.excludedReadPartitionsLock.Unlock()
	if jd.conf.numPartitions == 0 {
		// Partitioning is not enabled; nothing to read.
		return nil
	}

	query := `SELECT partition_id FROM ` + jd.tablePrefix + `_read_excluded_partitions`
	rows, err := jd.dbHandle.Query(query)
	if err != nil {
		return fmt.Errorf("querying read excluded partitions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var partitionName string
		if err := rows.Scan(&partitionName); err != nil {
			return fmt.Errorf("scanning read excluded partition: %w", err)
		}
		if jd.excludedReadPartitions == nil {
			jd.excludedReadPartitions = make(map[string]struct{})
		}
		jd.excludedReadPartitions[partitionName] = struct{}{}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("iterating read excluded partitions: %w", err)
	}
	return nil
}

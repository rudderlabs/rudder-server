package jobsdb

import (
	"context"
	"fmt"
	"sort"

	"github.com/lib/pq"
	"github.com/samber/lo"

	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
)

// applyMultiConsumerFlip handles both directions of the multi-consumer transition.
//
// Upgrade (multiConsumer=true): ensures every dataset in dsList has the v_last_c_ view
// and consumers registry table. Idempotent — already-migrated datasets are skipped.
// The registry table is the canonical marker: ConsumersTable != "" means fully migrated.
//
// Downgrade (multiConsumer=false): asserts that no dataset's registry contains a named
// (non-empty) consumer. Datasets whose registry holds only the legacy empty-string consumer
// are safe to ignore. Panics if named consumers are found, because consumer-scoped status
// data cannot be faithfully represented in the single-consumer schema.
func (jd *Handle) applyMultiConsumerFlip(ctx context.Context, tx *Tx, dsList []dataSetT) error {
	if jd.conf.multiConsumer {
		pending := lo.Filter(dsList, func(ds dataSetT, _ int) bool { return ds.ConsumersTable == "" })
		if len(pending) == 0 {
			return nil
		}
		return jd.withMaintenanceTx(ctx, func(mtx *Tx) error {
			for _, ds := range pending {
				viewName := "v_last_c_" + ds.JobStatusTable
				registry := ds.consumersRegistryTable()

				if _, err := mtx.ExecContext(ctx, fmt.Sprintf(
					`CREATE OR REPLACE VIEW %q AS SELECT DISTINCT ON (job_id, consumer) * FROM %q ORDER BY job_id ASC, consumer, id DESC`,
					viewName, ds.JobStatusTable,
				)); err != nil {
					return fmt.Errorf("creating view %s: %w", viewName, err)
				}
				// Create the registry and seed the legacy empty consumer in one shot.
				if _, err := mtx.ExecContext(ctx, fmt.Sprintf(
					`CREATE TABLE %q (consumer TEXT PRIMARY KEY); INSERT INTO %q VALUES ('')`,
					registry, registry,
				)); err != nil {
					return fmt.Errorf("creating registry table %s: %w", registry, err)
				}
			}
			return nil
		})
	}
	for _, ds := range dsList {
		if ds.ConsumersTable == "" {
			continue
		}
		var count int
		if err := tx.QueryRowContext(ctx,
			fmt.Sprintf(`SELECT COUNT(*) FROM %q WHERE consumer != ''`, ds.ConsumersTable),
		).Scan(&count); err != nil {
			return fmt.Errorf("checking consumers registry %q: %w", ds.ConsumersTable, err)
		}
		if count > 0 {
			return fmt.Errorf(
				"jobsdb %q: named consumers exist in %q but WithMultiConsumer() is not set — downgrade is unsupported",
				jd.tablePrefix, ds.ConsumersTable,
			)
		}
	}
	return nil
}

// registerConsumers inserts new consumer values from jobList into the dataset's
// registry table. Existing entries are silently ignored (ON CONFLICT DO NOTHING).
// Consumers are sorted before insertion for a stable, index-friendly, deadlock-avoiding order.
func (jd *Handle) registerConsumers(ctx context.Context, tx *Tx, ds dataSetT, jobList []*JobT) error {
	seen := map[string]struct{}{}
	for _, job := range jobList {
		consumers := job.Consumers
		if len(consumers) == 0 {
			seen[""] = struct{}{}
			continue
		}
		for _, c := range consumers {
			seen[c] = struct{}{}
		}
	}
	sorted := lo.Keys(seen)
	sort.Strings(sorted)
	_, err := tx.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO %q SELECT unnest($1::text[]) ON CONFLICT DO NOTHING`, ds.consumersRegistryTable()),
		pq.Array(sorted),
	)
	return err
}

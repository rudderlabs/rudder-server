package jobsdb

import (
	"context"
	"fmt"
	"sort"

	"github.com/lib/pq"
	"github.com/samber/lo"

	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
)

// applyMultiConsumerFlip ensures every dataset in dsList has the multi-consumer
// schema artifacts: the v_last_c_ view and the consumers registry table.
// It is idempotent — datasets that already carry both artifacts are skipped —
// so it is safe to call on every startup. Any dataset left incomplete by a prior
// crash will be finished on the next boot.
//
// The registry table is the canonical marker: if it exists (ConsumersTable != ""),
// the dataset is fully migrated. The view uses CREATE OR REPLACE to handle the
// rare case where a prior run created the view but crashed before the registry.
func (jd *Handle) applyMultiConsumerFlip(ctx context.Context, dsList []dataSetT) error {
	pending := lo.Filter(dsList, func(ds dataSetT, _ int) bool { return ds.ConsumersTable == "" })
	if len(pending) == 0 {
		return nil
	}
	return jd.withMaintenanceTx(ctx, func(tx *Tx) error {
		for _, ds := range pending {
			viewName := "v_last_c_" + ds.JobStatusTable
			registry := ds.consumersRegistryTable()

			if _, err := tx.ExecContext(ctx, fmt.Sprintf(
				`CREATE OR REPLACE VIEW %q AS SELECT DISTINCT ON (job_id, consumer) * FROM %q ORDER BY job_id ASC, consumer, id DESC`,
				viewName, ds.JobStatusTable,
			)); err != nil {
				return fmt.Errorf("creating view %s: %w", viewName, err)
			}
			if _, err := tx.ExecContext(ctx, fmt.Sprintf(
				`CREATE TABLE %q (consumer TEXT PRIMARY KEY)`, registry,
			)); err != nil {
				return fmt.Errorf("creating registry table %s: %w", registry, err)
			}
			// All pre-existing jobs are legacy, so the only consumer is ''.
			if _, err := tx.ExecContext(ctx, fmt.Sprintf(
				`INSERT INTO %q VALUES ('')`, registry,
			)); err != nil {
				return fmt.Errorf("seeding registry table %s: %w", registry, err)
			}
		}
		return nil
	})
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

// assertNoMultiConsumerDowngrade panics at startup if any dataset has a consumers
// registry table while WithMultiConsumer() is not set. Downgrade is one-way:
// once a handle has been flipped, turning the option off is unsupported because
// consumer-scoped status data cannot be faithfully represented in the single-consumer schema.
func (jd *Handle) assertNoMultiConsumerDowngrade(dsList []dataSetT) {
	for _, ds := range dsList {
		if ds.ConsumersTable != "" {
			panic(fmt.Errorf(
				"jobsdb %q: multi-consumer schema artifacts exist but WithMultiConsumer() is not set — downgrade is unsupported",
				jd.tablePrefix,
			))
		}
	}
}

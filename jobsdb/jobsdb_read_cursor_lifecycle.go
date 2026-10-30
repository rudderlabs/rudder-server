package jobsdb

import (
	"context"
	"fmt"
	"time"

	"github.com/lib/pq"
	"golang.org/x/sync/errgroup"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/utils/crash"
)

func (rc *readCursor) Start() error {
	if err := rc.JobsDB.Start(); err != nil {
		return err
	}
	rc.lifecycle.mu.Lock()
	defer rc.lifecycle.mu.Unlock()
	if rc.lifecycle.started {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := rc.load(ctx); err != nil {
		cancel()
		rc.JobsDB.Stop()
		return fmt.Errorf("loading read cursors of %q: %w", rc.h.tablePrefix, err)
	}
	g := &errgroup.Group{}
	g.Go(crash.Wrapper(func() error {
		rc.persistLoop(ctx)
		return nil
	}))
	rc.lifecycle.started, rc.lifecycle.cancel, rc.lifecycle.group = true, cancel, g
	return nil
}

func (rc *readCursor) Stop() {
	rc.lifecycle.mu.Lock()
	if rc.lifecycle.started {
		rc.lifecycle.cancel()
		_ = rc.lifecycle.group.Wait()
		rc.lifecycle.started = false
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := rc.persist(ctx); err != nil {
			rc.log.Warnn("Persisting read cursors on stop", obskit.Error(err))
		}
		cancel()
	}
	rc.lifecycle.mu.Unlock()
	rc.JobsDB.Stop()
}

func (jd *Handle) readCursorsTable() string {
	return jd.tablePrefix + "_read_cursors"
}

func (rc *readCursor) load(ctx context.Context) error {
	rows, err := rc.h.maintenanceDB().QueryContext(ctx, `SELECT key, job_id FROM `+rc.h.readCursorsTable())
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			persisted string
			watermark int64
		)
		if err := rows.Scan(&persisted, &watermark); err != nil {
			return err
		}
		key, ok := parsePipelineKey(persisted)
		if !ok {
			continue
		}
		p := rc.getPipeline(key)
		p.mu.Lock()
		p.watermark = max(p.watermark, watermark)
		p.mu.Unlock()
		p.applied.Store(watermark)
	}
	return rows.Err()
}

func (rc *readCursor) persistLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(rc.conf.persistInterval.Load()):
		}
		disabled := rc.disabled.Load() // so this persist truncates the persisted watermarks
		err := rc.persist(ctx)
		if err == nil && disabled {
			return
		}
		if err != nil && ctx.Err() == nil {
			rc.log.Warnn("Persisting read cursors", obskit.Error(err))
		}
	}
}

// persist replaces the persisted watermarks with the ones that skip at least the first
// persistMinSkippedDatasets sealed datasets, once there are more sealed datasets than that, and applies
// them once committed. A disabled decorator persists none.
func (rc *readCursor) persist(ctx context.Context) error {
	var (
		keys       []string
		watermarks []int64
		applied    = map[*pipeline]int64{}
	)
	n := rc.conf.persistMinSkippedDatasets.Load()
	if _, dsRangeList := rc.h.dsList.snapshot(); !rc.disabled.Load() && len(dsRangeList) > n {
		threshold := dsRangeList[n-1].maxJobID
		rc.pipelinesMu.RLock()
		for key, p := range rc.pipelines {
			if watermark := p.getWatermark(); watermark > threshold {
				keys, watermarks = append(keys, key.String()), append(watermarks, watermark)
				applied[p] = watermark
			}
		}
		rc.pipelinesMu.RUnlock()
	}
	table := rc.h.readCursorsTable()
	tx, err := rc.h.maintenanceDB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `TRUNCATE `+table); err != nil {
		return err
	}
	if len(keys) > 0 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO `+table+` (key, job_id) SELECT * FROM unnest($1::text[], $2::bigint[])`,
			pq.Array(keys), pq.Array(watermarks),
		); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	rc.pipelinesMu.RLock()
	defer rc.pipelinesMu.RUnlock()
	for _, p := range rc.pipelines {
		p.applied.Store(applied[p])
	}
	return nil
}

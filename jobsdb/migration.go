package jobsdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb/internal/dsindex"
	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
	"github.com/rudderlabs/rudder-server/utils/misc"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
)

// startMigrateDSLoop migrates jobs from src dataset (srcDS) to destination dataset (dest_ds)
// First all the unprocessed jobs are copied over. Then all the jobs which haven't
// completed (state is failed or waiting or waiting_retry or executiong) are copied
// over. Then the status (only the latest) is set for those jobs
func (jd *Handle) startMigrateDSLoop(ctx context.Context) {
	jd.backgroundGroup.Go(misc.WithBugsnag(func() error {
		jd.migrateDSLoop(ctx)
		return nil
	}))
}

func (jd *Handle) migrateDSLoop(ctx context.Context) {
	for {
		select {
		case <-jd.TriggerMigrateDS():
		case <-ctx.Done():
			return
		}
		migrate := func() error {
			start := time.Now()
			jd.logger.Debugw("Start", "operation", "migrateDSLoop")
			timeoutCtx, cancel := context.WithTimeout(ctx, jd.conf.migration.migrateDSTimeout.Load())
			defer cancel()
			err := jd.doMigrateDS(timeoutCtx)
			stats.Default.NewTaggedStat("migration_loop", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix, "error": strconv.FormatBool(err != nil)}).Since(start)
			if err != nil {
				return fmt.Errorf("failed to migrate ds: %w", err)
			}
			return nil
		}
		if err := migrate(); err != nil && ctx.Err() == nil {
			if !jd.conf.skipMaintenanceError {
				panic(err)
			}
			jd.logger.Errorw("Failed to migrate ds", "error", err)
		}
	}
}

func (jd *Handle) doMigrateDS(ctx context.Context) error {
	if !jd.dsListLock.RTryLockWithCtx(ctx) {
		return fmt.Errorf("could not acquire a dslist read lock: %w", ctx.Err())
	}
	dsList := jd.getDSList()
	jd.dsListLock.RUnlock()

	if err := jd.cleanupStatusTables(ctx, dsList); err != nil {
		return err
	}

	migrateFrom, pendingJobsCount, insertBeforeDS, err := jd.getMigrationList(dsList)
	if err != nil {
		return fmt.Errorf("could not get migration list: %w", err)
	}
	if len(migrateFrom) == 0 {
		return nil
	}
	var l lock.LockToken
	var lockChan chan<- lock.LockToken

	lockStart := time.Now()
	err = jd.WithTx(func(tx *Tx) error {
		return jd.withDistributedSharedLock(ctx, tx, "schema_migrate", func() error { // cannot run while schema migration is running
			// Take the lock and run actual migration
			if !jd.dsMigrationLock.TryLockWithCtx(ctx) {
				return fmt.Errorf("failed to acquire lock: %w", ctx.Err())
			}
			defer jd.dsMigrationLock.Unlock()
			// repeat the check after the dsMigrationLock is acquired to get correct pending jobs count.
			// the pending jobs count cannot change after the dsMigrationLock is acquired
			migrateFrom, pendingJobsCount, insertBeforeDS, err = jd.getMigrationList(dsList)
			if err != nil {
				return fmt.Errorf("could not get migration list: %w", err)
			}
			if len(migrateFrom) == 0 {
				return nil
			}

			if pendingJobsCount > 0 { // migrate incomplete jobs
				var destination dataSetT
				if err := jd.dsListLock.WithLockInCtx(ctx, func(l lock.LockToken) error {
					dsIdx, err := jd.computeNewIdxForIntraNodeMigration(l, insertBeforeDS)
					if err != nil {
						return fmt.Errorf("computing new index for intra-node migration: %w", err)
					}
					destination = newDataSet(jd.tablePrefix, dsIdx)
					return nil
				}); err != nil {
					return err
				}

				jd.logger.Infof("[[ migrateDSLoop ]]: Migrate from: %v", migrateFrom)
				jd.logger.Infof("[[ migrateDSLoop ]]: To: %v", destination)
				jd.logger.Infof("[[ migrateDSLoop ]]: Next: %v", insertBeforeDS)

				opPayload, err := json.Marshal(&journalOpPayloadT{From: migrateFrom, To: destination})
				if err != nil {
					return fmt.Errorf("failed to marshal journal payload: %w", err)
				}

				opID, err := jd.JournalMarkStartInTx(tx, migrateCopyOperation, opPayload)
				if err != nil {
					return fmt.Errorf("failed to mark journal start: %w", err)
				}

				err = jd.addDSInTx(tx, destination)
				if err != nil {
					return fmt.Errorf("failed to add dataset: %w", err)
				}

				totalJobsMigrated := 0
				var noJobsMigrated int
				for _, source := range migrateFrom {
					jd.logger.Infof("[[ migrateDSLoop ]]: Migrate: %v to: %v", source, destination)
					noJobsMigrated, err = jd.migrateJobsInTx(ctx, tx, source, destination)
					if err != nil {
						return fmt.Errorf("failed to migrate jobs: %w", err)
					}
					totalJobsMigrated += noJobsMigrated
				}
				if err = jd.journalMarkDoneInTx(tx, opID); err != nil {
					return fmt.Errorf("failed to mark journal done: %w", err)
				}
				jd.logger.Infof("[[ migrateDSLoop ]]: Total migrated %d jobs", totalJobsMigrated)
			}

			opPayload, err := json.Marshal(&journalOpPayloadT{From: migrateFrom})
			if err != nil {
				return fmt.Errorf("failed to marshal journal payload: %w", err)
			}
			opID, err := jd.JournalMarkStartInTx(tx, postMigrateDSOperation, opPayload)
			if err != nil {
				return fmt.Errorf("failed to mark journal start: %w", err)
			}
			// acquire an async lock, as this needs to be released after the transaction commits
			l, lockChan, err = jd.dsListLock.AsyncLockWithCtx(ctx)
			if err != nil {
				return fmt.Errorf("failed to acquire lock: %w", err)
			}
			if err = jd.postMigrateHandleDS(tx, migrateFrom); err != nil {
				return fmt.Errorf("failed to post migrate handle ds: %w", err)
			}
			if err = jd.journalMarkDoneInTx(tx, opID); err != nil {
				return fmt.Errorf("failed to mark journal done: %w", err)
			}
			return nil
		})
	})
	if l != nil {
		defer stats.Default.NewTaggedStat("migration_loop_lock", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix}).Since(lockStart)
		defer func() { lockChan <- l }()
		if err == nil {
			if err = jd.doRefreshDSRangeList(l); err != nil {
				return fmt.Errorf("failed to refresh ds range list: %w", err)
			}
		}

	}
	return err
}

// based on size of given DSs, gives a list of DSs for us to vacuum full status tables
func (jd *Handle) getVacuumFullCandidates(ctx context.Context, dsList []dataSetT) ([]string, error) {
	// get name and it's size of all tables
	var rows *sql.Rows
	rows, err := jd.dbHandle.QueryContext(
		ctx,
		`SELECT pg_table_size(oid) AS size, relname
		FROM pg_class
		where relname = ANY(
			SELECT tablename
				FROM pg_catalog.pg_tables
				WHERE schemaname NOT IN ('pg_catalog','information_schema')
				AND tablename like $1
		) order by relname;`,
		jd.tablePrefix+"_job_status%",
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	tableSizes := map[string]int64{}
	for rows.Next() {
		var (
			tableSize int64
			tableName string
		)
		err = rows.Scan(&tableSize, &tableName)
		if err != nil {
			return nil, err
		}
		tableSizes[tableName] = tableSize
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	toVacuumFull := []string{}
	for _, ds := range dsList {
		if tableSizes[ds.JobStatusTable] > jd.conf.migration.vacuumFullStatusTableThreshold() {
			toVacuumFull = append(toVacuumFull, ds.JobStatusTable)
		}
	}
	return toVacuumFull, nil
}

// based on an estimate of the rows in DSs, gives a list of DSs for us to cleanup status tables
func (jd *Handle) getCleanUpCandidates(ctx context.Context, dsList []dataSetT) ([]dataSetT, error) {
	// get analyzer estimates for the number of rows(jobs, statuses) in each DS
	var rows *sql.Rows
	rows, err := jd.dbHandle.QueryContext(
		ctx,
		`SELECT reltuples AS estimate, relname
		FROM pg_class
		where relname = ANY(
			SELECT tablename
				FROM pg_catalog.pg_tables
				WHERE schemaname NOT IN ('pg_catalog','information_schema')
				AND tablename like $1
		)`,
		jd.tablePrefix+"_job%",
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	estimates := map[string]float64{}
	for rows.Next() {
		var (
			estimate  float64
			tableName string
		)
		if err = rows.Scan(&estimate, &tableName); err != nil {
			return nil, err
		}
		estimates[tableName] = estimate
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	datasets := lo.Filter(dsList,
		func(ds dataSetT, idx int) bool {
			statuses := estimates[ds.JobStatusTable]
			jobs := estimates[ds.JobTable]
			if jobs == 0 { // using max ds size if we have no stats for the number of jobs
				jobs = float64(jd.conf.MaxDSSize.Load())
			}
			return statuses/jobs > jd.conf.migration.jobStatusMigrateThres()
		})

	return lo.Slice(datasets, 0, jd.conf.migration.maxMigrateDSProbe.Load()), nil
}

// based on an estimate cleans up the status tables
func (jd *Handle) cleanupStatusTables(ctx context.Context, dsList []dataSetT) error {
	var toVacuum []string
	toVacuumFull, err := jd.getVacuumFullCandidates(ctx, dsList)
	if err != nil {
		return err
	}
	toVacuumFullMap := lo.Associate(toVacuumFull, func(k string) (string, struct{}) {
		return k, struct{}{}
	})
	toCompact, err := jd.getCleanUpCandidates(ctx, dsList)
	if err != nil {
		return err
	}
	start := time.Now()
	defer stats.Default.NewTaggedStat(
		"jobsdb_compact_status_tables",
		stats.TimerType,
		stats.Tags{"customVal": jd.tablePrefix},
	).Since(start)

	if err := jd.WithTx(func(tx *Tx) error {
		for _, statusTable := range toCompact {
			table := statusTable.JobStatusTable
			// clean up and vacuum if not present in toVacuumFullMap
			_, ok := toVacuumFullMap[table]
			vacuum, err := jd.cleanStatusTable(ctx, tx, table, !ok)
			if err != nil {
				return err
			}
			if vacuum {
				toVacuum = append(toVacuum, table)
			}
		}

		return nil
	}); err != nil {
		return err
	}
	// vacuum full
	for _, table := range toVacuumFull {
		jd.logger.Infof("vacuuming full %q", table)
		if _, err := jd.dbHandle.ExecContext(ctx, fmt.Sprintf(`VACUUM FULL %[1]q`, table)); err != nil {
			return err
		}
	}
	// vacuum analyze
	for _, table := range toVacuum {
		jd.logger.Infof("vacuuming %q", table)
		if _, err := jd.dbHandle.ExecContext(ctx, fmt.Sprintf(`VACUUM ANALYZE %[1]q`, table)); err != nil {
			return err
		}
	}
	return nil
}

// cleanStatusTable deletes all rows except for the latest status for each job
func (jd *Handle) cleanStatusTable(ctx context.Context, tx *Tx, table string, canBeVacuumed bool) (vacuum bool, err error) {
	result, err := tx.ExecContext(
		ctx,
		fmt.Sprintf(`DELETE FROM %[1]q
						WHERE NOT id = ANY(
							SELECT DISTINCT ON (job_id) id from "%[1]s" ORDER BY job_id ASC, id DESC
						)`, table),
	)
	if err != nil {
		return false, err
	}

	numJobStatusDeleted, err := result.RowsAffected()
	if err != nil {
		return false, err
	}

	if numJobStatusDeleted > jd.conf.migration.vacuumAnalyzeStatusTableThreshold() && canBeVacuumed {
		vacuum = true
	} else {
		_, err = tx.ExecContext(ctx, fmt.Sprintf(`ANALYZE %q`, table))
	}

	return
}

// getMigrationList returns the list of datasets to migrate from,
// the number of unfinished jobs contained in these datasets
// and the dataset before which the new (migrated) dataset that will hold these jobs needs to be created
func (jd *Handle) getMigrationList(dsList []dataSetT) (migrateFrom []dataSetT, pendingJobsCount int, insertBeforeDS dataSetT, err error) {
	var (
		liveDSCount, migrateDSProbeCount int
		// we don't want `maxDSSize` value to change, during dsList loop
		maxDSSize = jd.conf.MaxDSSize.Load()
		waiting   *smallDS
	)

	jd.logger.Debugf("[[ migrateDSLoop ]]: DS list %+v", dsList)

	for idx, ds := range dsList {
		var idxCheck bool
		if jd.ownerType == Read {
			// if jobsdb owner is read, exempting the last two datasets from migration.
			// This is done to avoid dsList conflicts between reader and writer
			idxCheck = idx == len(dsList)-1 || idx == len(dsList)-2
		} else {
			idxCheck = idx == len(dsList)-1
		}

		if liveDSCount >= jd.conf.migration.maxMigrateOnce.Load() || pendingJobsCount >= maxDSSize || idxCheck {
			break
		}

		migrate, isSmall, recordsLeft, migrateErr := jd.checkIfMigrateDS(ds)
		if migrateErr != nil {
			err = migrateErr
			return
		}
		jd.logger.Debugf(
			"[[ migrateDSLoop ]]: Migrate check %v, is small: %v, records left: %d, ds: %v",
			migrate, isSmall, recordsLeft, ds,
		)

		if migrate {
			if waiting != nil { // add current and waiting DS, no matter if the current ds is small or not, it doesn't matter
				migrateFrom = append(migrateFrom, waiting.ds, ds)
				insertBeforeDS = dsList[idx+1]
				pendingJobsCount += waiting.recordsLeft + recordsLeft
				liveDSCount += 2
				waiting = nil
			} else if !isSmall || len(migrateFrom) > 0 { // add only if the current DS is not small or if we already have some DS in the list
				migrateFrom = append(migrateFrom, ds)
				insertBeforeDS = dsList[idx+1]
				pendingJobsCount += recordsLeft
				liveDSCount++
			} else { // add the current small DS as waiting for the next iteration to pickup
				waiting = &smallDS{ds: ds, recordsLeft: recordsLeft}
			}
		} else {
			waiting = nil // if there was a small DS waiting, we should remove it since its next dataset is not eligible for migration
			if liveDSCount > 0 || migrateDSProbeCount > jd.conf.migration.maxMigrateDSProbe.Load() {
				// DS is not eligible for migration. But there are data sets on the left eligible to migrate, so break.
				break
			}
		}
		migrateDSProbeCount++
	}
	return
}

func (jd *Handle) migrateJobsInTx(ctx context.Context, tx *Tx, srcDS, destDS dataSetT) (int, error) {
	defer jd.getTimerStat(
		"migration_jobs",
		&statTags{CustomValFilters: []string{jd.tablePrefix}},
	).RecordDuration()()

	compactDSQuery := fmt.Sprintf(
		`with last_status as (select * from "v_last_%[1]s"),
		inserted_jobs as
		(
			insert into %[3]q (job_id,   workspace_id,   uuid,   user_id,   custom_val,   parameters,   event_payload,   event_count,   created_at,   expire_at)
			           (select j.job_id, j.workspace_id, j.uuid, j.user_id, j.custom_val, j.parameters, j.event_payload, j.event_count, j.created_at, j.expire_at from %[2]q j left join last_status js on js.job_id = j.job_id
				where js.job_id is null or js.job_state = ANY('{%[5]s}') order by j.job_id) returning job_id
		),
		insertedStatuses as
		(
			insert into %[4]q (job_id, job_state, attempt, exec_time, retry_time, error_code, error_response, parameters)
			           (select job_id, job_state, attempt, exec_time, retry_time, error_code, error_response, parameters from last_status where job_state = ANY('{%[5]s}'))
		)
		select count(*) from inserted_jobs;`,
		srcDS.JobStatusTable,
		srcDS.JobTable,
		destDS.JobTable,
		destDS.JobStatusTable,
		strings.Join(validNonTerminalStates, ","),
	)

	var numJobsMigrated int64
	if err := tx.QueryRowContext(
		ctx,
		compactDSQuery,
	).Scan(&numJobsMigrated); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(fmt.Sprintf(`ANALYZE %q, %q`, destDS.JobTable, destDS.JobStatusTable)); err != nil {
		return 0, err
	}
	return int(numJobsMigrated), nil
}

func (jd *Handle) computeNewIdxForIntraNodeMigration(l lock.LockToken, insertBeforeDS dataSetT) (string, error) { // Within the node
	jd.logger.Debugf("computeNewIdxForIntraNodeMigration, insertBeforeDS : %v", insertBeforeDS)
	dList, err := jd.doRefreshDSList(l)
	if err != nil {
		return "", fmt.Errorf("refreshDSList: %w", err)
	}
	jd.logger.Debugf("dlist in which we are trying to find %v is %v", insertBeforeDS, dList)
	newDSIdx := ""
	jd.assert(len(dList) > 0, fmt.Sprintf("len(dList): %d <= 0", len(dList)))
	for idx, ds := range dList {
		if ds.Index == insertBeforeDS.Index {
			jd.assert(idx > 0, "We never want to insert before first dataset")
			newDSIdx, err = computeInsertIdx(dList[idx-1].Index, insertBeforeDS.Index)
			jd.assertError(err)
		}
	}
	return newDSIdx, nil
}

func (jd *Handle) postMigrateHandleDS(tx *Tx, migrateFrom []dataSetT) error {
	// Rename datasets before dropping them, so that they can be uploaded to s3
	for _, ds := range migrateFrom {
		if jd.isBackupEnabled() {
			jd.logger.Debugf("renaming dataset %s to %s", ds.JobTable, ds.JobTable+preDropTablePrefix+ds.JobTable)
			if err := jd.mustRenameDSInTx(tx, ds); err != nil {
				return err
			}
		} else {
			jd.logger.Debugf("dropping dataset %s", ds.JobTable)
			if err := jd.dropDSInTx(tx, ds); err != nil {
				return err
			}
		}
	}
	return nil
}

func computeInsertIdx(beforeIndex, afterIndex string) (string, error) {
	before, err := dsindex.Parse(beforeIndex)
	if err != nil {
		return "", fmt.Errorf("could not parse before index: %w", err)
	}
	after, err := dsindex.Parse(afterIndex)
	if err != nil {
		return "", fmt.Errorf("could not parse after index: %w", err)
	}
	result, err := before.Bump(after)
	if err != nil {
		return "", fmt.Errorf("could not compute insert index: %w", err)
	}
	if result.Length() > 2 {
		return "", fmt.Errorf("unsupported resulting index %s level (3) between %s and %s", result, beforeIndex, afterIndex)
	}
	return result.String(), nil
}

// checkIfMigrateDS checks when DB is full or DB needs to be migrated.
// We migrate the DB ONCE most of the jobs have been processed (succeeded/aborted)
// Or when the job_status table gets too big because of lots of retries/failures
func (jd *Handle) checkIfMigrateDS(ds dataSetT) (
	migrate, small bool, recordsLeft int, err error,
) {
	defer jd.getTimerStat(
		"migration_ds_check",
		&statTags{CustomValFilters: []string{jd.tablePrefix}},
	).RecordDuration()()

	var delCount, totalCount int
	sqlStatement := fmt.Sprintf(`SELECT COUNT(*) from %q`, ds.JobTable)
	if err = jd.dbHandle.QueryRow(sqlStatement).Scan(&totalCount); err != nil {
		return false, false, 0, fmt.Errorf("error getting count of jobs in %s: %w", ds.JobTable, err)
	}

	// Jobs which have either succeeded or expired
	sqlStatement = fmt.Sprintf(`SELECT COUNT(DISTINCT(job_id))
                                      from %q
                                      WHERE job_state IN ('%s')`,
		ds.JobStatusTable, strings.Join(validTerminalStates, "', '"))
	if err = jd.dbHandle.QueryRow(sqlStatement).Scan(&delCount); err != nil {
		return false, false, 0, fmt.Errorf("error getting count of jobs in %s: %w", ds.JobStatusTable, err)
	}

	recordsLeft = totalCount - delCount

	if jd.conf.minDSRetentionPeriod.Load() > 0 {
		var maxCreatedAt time.Time
		sqlStatement = fmt.Sprintf(`SELECT MAX(created_at) from %q`, ds.JobTable)
		if err = jd.dbHandle.QueryRow(sqlStatement).Scan(&maxCreatedAt); err != nil {
			return false, false, 0, fmt.Errorf("error getting max created_at from %s: %w", ds.JobTable, err)
		}

		if time.Since(maxCreatedAt) < jd.conf.minDSRetentionPeriod.Load() {
			return false, false, recordsLeft, nil
		}
	}

	if jd.conf.maxDSRetentionPeriod.Load() > 0 {
		var terminalJobsExist bool
		sqlStatement = fmt.Sprintf(`SELECT EXISTS (
									SELECT id
										FROM %q
										WHERE job_state = ANY($1) and exec_time < $2)`,
			ds.JobStatusTable)
		if err = jd.dbHandle.QueryRow(sqlStatement, pq.Array(validTerminalStates), time.Now().Add(-1*jd.conf.maxDSRetentionPeriod.Load())).Scan(&terminalJobsExist); err != nil {
			return false, false, 0, fmt.Errorf("checking terminalJobsExist %s: %w", ds.JobStatusTable, err)
		}
		if terminalJobsExist {
			return true, false, recordsLeft, nil
		}
	}

	smallThreshold := jd.conf.migration.jobMinRowsMigrateThres() * float64(jd.conf.MaxDSSize.Load())
	isSmall := func() bool {
		return float64(totalCount) < smallThreshold
	}

	if float64(delCount)/float64(totalCount) > jd.conf.migration.jobDoneMigrateThres() {
		return true, isSmall(), recordsLeft, nil
	}

	if isSmall() {
		return true, true, recordsLeft, nil
	}

	return false, false, recordsLeft, nil
}

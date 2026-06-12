package jobsdb

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/collectors"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/jobsdb/internal/cache"
	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
	"github.com/rudderlabs/rudder-server/utils/crash"
	"github.com/rudderlabs/rudder-server/utils/misc"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
)

/*
Setup initializes and starts a Handle.
clearAll removes existing tables for writer handles.
tablePrefix must be unique and separates multiple JobsDB instances.
*/
func (jd *Handle) Setup(
	ownerType OwnerType, clearAll bool, tablePrefix string,
) error {
	jd.ownerType = ownerType
	jd.conf.clearAll = clearAll
	jd.tablePrefix = tablePrefix
	jd.init()
	return jd.Start()
}

func (jd *Handle) init() {
	jd.dsList = newVersionedDSList(nil, nil)
	jd.dropNotify = make(chan struct{}, 1)
	if jd.logger == nil {
		jd.logger = logger.NewLogger().Child("jobsdb").Child(jd.tablePrefix)
	}
	jd.dsRangeFuncMap = make(map[string]func() (dsRangeMinMax, error))
	jd.distinctValuesCache = NewDistinctValuesCache()

	if jd.config == nil {
		jd.config = config.Default
	}

	if string(jd.conf.payloadColumnType) == "" {
		jd.conf.payloadColumnType = TEXT
	}

	if jd.stats == nil {
		jd.stats = stats.Default
	}
	jd.dsListLock = lock.NewLocker("dsListLock", jd.tablePrefix, jd.stats)
	jd.dsCompactionLock = lock.NewLocker("dsCompactionLock", jd.tablePrefix, jd.stats)

	jd.loadConfig()

	// Initialize dbHandle if not already set
	if jd.dbHandle != nil {
		jd.sharedConnectionPool = true
	} else {
		var err error
		psqlInfo := misc.GetConnectionString(jd.config, "jobsdb_"+jd.tablePrefix)
		jd.dbHandle, err = sql.Open("postgres", psqlInfo)
		jd.assertError(err)

		jd.assertError(
			jd.stats.RegisterCollector(
				collectors.NewDatabaseSQLStats(
					"jobsdb_"+jd.tablePrefix+"_"+jd.ownerType.Identifier(),
					jd.dbHandle,
				),
			),
		)

		var maxConns int
		if !jd.conf.enableReaderQueue || !jd.conf.enableWriterQueue {
			maxConns = jd.conf.maxOpenConnections
		} else {
			maxConns = 2 // buffer
			maxConns += jd.conf.maxReaders + jd.conf.maxWriters
			switch jd.ownerType {
			case Read:
				maxConns += 3 // compact, refreshDsList, dropDS
			case Write:
				maxConns += 1 // addNewDS
			case ReadWrite:
				maxConns += 3 // compact, addNewDS, dropDS
			}
			if maxConns >= jd.conf.maxOpenConnections {
				maxConns = jd.conf.maxOpenConnections
			}
		}
		jd.dbHandle.SetMaxOpenConns(maxConns)

		jd.assertError(jd.dbHandle.Ping())
	}

	jd.workersAndAuxSetup()

	err := jd.WithTx(context.Background(), func(tx *Tx) error {
		// only one migration should run at a time and block all other processes from adding or removing tables
		return jd.withDistributedLock(context.Background(), tx, "schema_migrate", func() error {
			// Database schema migration should happen early, even before jobsdb is started,
			// so that we can be sure that all the necessary tables are created and considered to be in
			// the latest schema version, before rudder-migrator starts introducing new tables.
			jd.dsListLock.WithLock(func(l lock.LockToken) {
				writer := jd.ownerType == Write || jd.ownerType == ReadWrite
				if writer && jd.conf.clearAll {
					jd.dropDatabaseTables(l)
				}

				// Important: if jobsdb type is acting as a writer then refreshDSList
				// doesn't return the full list of datasets, only the rightmost two.
				// But we need to run the schema migration against all datasets, no matter
				// whether jobsdb is a writer or not.
				allDatasets, err := getDSList(jd, tx, jd.tablePrefix)
				jd.assertError(err)

				datasetIndices := make([]string, len(allDatasets))
				for i, ds := range allDatasets {
					datasetIndices[i] = ds.Index
				}
				templateData := map[string]any{
					"Prefix":              jd.tablePrefix,
					"Datasets":            datasetIndices,
					"PartitioningEnabled": jd.conf.numPartitions > 0,
				}

				// One-way guard: opt-in check that panics if MC artifacts exist without WithMultiConsumer().
				if !jd.conf.multiConsumer && jd.conf.disallowMultiConsumerDowngrade {
					jd.assertNoMultiConsumerDowngrade(allDatasets)
				}

				if writer {
					jd.setupDatabaseTables(templateData)
				}

				// Run changesets that should always run for both writer and reader jobsdbs.
				//
				// When running separate gw and processor instances we cannot control the order of execution
				// and we cannot guarantee that after a gw migration completes, processor
				// will not create new tables using the old schema.
				//
				// Changesets that run always can help in such cases, by bringing non-migrated tables into a usable state.
				jd.runAlwaysChangesets(templateData)

				// For multi-consumer handles, ensure every existing dataset has the
				// v_last_c_ view and consumers registry table. This is the flip path:
				// runs on first boot after WithMultiConsumer() is set, and is a no-op
				// on subsequent boots once all datasets are up to date.
				if jd.conf.multiConsumer {
					jd.assertError(jd.applyMultiConsumerFlip(context.Background(), allDatasets))
				}

				// finally refresh the dataset list to make sure [datasetList] field is populated
				err = jd.doRefreshDSRangeListWithDB(l, jd.dbHandle)
				jd.assertError(err)
			})
			return nil
		})
	})
	if err != nil {
		panic(fmt.Errorf("failed to run schema migration for %s: %w", jd.tablePrefix, err))
	}
}

func (jd *Handle) workersAndAuxSetup() {
	jd.assert(jd.tablePrefix != "", "tablePrefix received is empty")

	var defaultLogCacheBranchInvalidation bool
	switch jd.tablePrefix {
	case "gw", "rt", "batch_rt", "arc":
		defaultLogCacheBranchInvalidation = true
	}
	cacheParams := cacheParameterFilters
	if jd.conf.multiConsumer {
		cacheParams = append(cacheParams, consumerParamName)
	}
	jd.noResultsCache = cache.NewNoResultsCache(
		cacheParams,
		func() time.Duration { return jd.conf.cacheExpiration.Load() },
		cache.WithWarnOnBranchInvalidation[ParameterFilterT](
			jd.config.GetReloadableBoolVar(defaultLogCacheBranchInvalidation, jd.configKeys("logCacheBranchInvalidation")...),
			jd.logger),
	)

	jd.logger.Infon("Connected to DB")
	jd.statPreDropTableCount = jd.stats.NewTaggedStat("jobsdb.pre_drop_tables_count", stats.GaugeType, stats.Tags{"customVal": jd.tablePrefix})
	jd.statTableCount = jd.stats.NewTaggedStat("jobsdb.tables_count", stats.GaugeType, stats.Tags{"customVal": jd.tablePrefix})
	jd.statNewDSPeriod = jd.stats.NewTaggedStat("jobsdb.new_ds_period", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	jd.statDropDSPeriod = jd.stats.NewTaggedStat("jobsdb.drop_ds_period", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	jd.statReadExcludedPartitionsCount = jd.stats.NewTaggedStat("jobsdb_read_excluded_partitions_count", stats.GaugeType, stats.Tags{"customVal": jd.tablePrefix})
}

// Start starts the jobsdb background workers for this handle's owner type.
// Start should be called before any other jobsdb methods are called.
func (jd *Handle) Start() error {
	jd.lifecycle.mu.Lock()
	defer jd.lifecycle.mu.Unlock()
	if jd.lifecycle.started {
		return nil
	}
	defer func() { jd.lifecycle.started = true }()

	jd.conf.writeCapacity = make(chan struct{}, jd.conf.maxWriters)
	jd.conf.readCapacity = make(chan struct{}, jd.conf.maxReaders)

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	jd.backgroundCancel = cancel
	jd.backgroundGroup = g

	jd.setUpForOwnerType(ctx, jd.ownerType)
	return nil
}

func (jd *Handle) setUpForOwnerType(ctx context.Context, ownerType OwnerType) {
	jd.dsListLock.WithLock(func(l lock.LockToken) {
		switch ownerType {
		case Read:
			jd.readerSetup(ctx, l)
		case Write:
			jd.writerSetup(ctx, l)
		case ReadWrite:
			jd.readerWriterSetup(ctx, l)
		}
	})
}

func (jd *Handle) readerSetup(ctx context.Context, l lock.LockToken) {
	jd.recoverFromJournal(Read)

	// This is a thread-safe operation.
	// Even if two different services (gateway and processor) perform this operation, there should not be any problem.
	jd.recoverFromJournal(ReadWrite)
	jd.assertError(func() error {
		err := jd.cleanupPreDropTables(ctx)
		if err != nil && ctx.Err() == nil {
			return err
		}
		return nil
	}())
	jd.assertError(jd.doRefreshDSRangeList(l))
	jd.assertError(func() error {
		err := jd.doCleanup(ctx, l)
		if err != nil && ctx.Err() == nil {
			return err
		}
		return nil
	}())
	jd.assertError(jd.loadReadExcludedPartitions())

	g := jd.backgroundGroup
	g.Go(crash.Wrapper(func() error {
		jd.refreshDSListLoop(ctx)
		return nil
	}))

	jd.startCompactionLoop(ctx)
	jd.startDropDSLoop(ctx)
}

func (jd *Handle) writerSetup(ctx context.Context, l lock.LockToken) {
	jd.recoverFromJournal(Write)
	// This is a thread-safe operation.
	// Even if two different services (gateway and processor) perform this operation, there should not be any problem.
	jd.recoverFromJournal(ReadWrite)
	jd.assertError(jd.doRefreshDSRangeList(l))

	// If no DS present, add one
	dsList, _ := jd.dsList.snapshot()
	if len(dsList) == 0 {
		jd.addNewDS(ctx, l, newDataSet(jd.tablePrefix, jd.computeNewIdxForAppend(l)))
	}

	jd.backgroundGroup.Go(crash.Wrapper(func() error {
		jd.addNewDSLoop(ctx)
		return nil
	}))
}

func (jd *Handle) readerWriterSetup(ctx context.Context, l lock.LockToken) {
	jd.recoverFromJournal(Read)

	jd.writerSetup(ctx, l)
	jd.assertError(func() error {
		err := jd.cleanupPreDropTables(ctx)
		if err != nil && ctx.Err() == nil {
			return err
		}
		return nil
	}())
	jd.assertError(func() error {
		err := jd.doCleanup(ctx, l)
		if err != nil && ctx.Err() == nil {
			return err
		}
		return nil
	}())
	jd.assertError(jd.loadReadExcludedPartitions())

	jd.startCompactionLoop(ctx)
	jd.startDropDSLoop(ctx)
}

// Stop stops the background goroutines and waits until they finish.
// Stop should be called once only after Start.
// Only Start and Close can be called after Stop.
func (jd *Handle) Stop() {
	jd.lifecycle.mu.Lock()
	defer jd.lifecycle.mu.Unlock()
	if jd.lifecycle.started {
		defer func() { jd.lifecycle.started = false }()
		jd.backgroundCancel()
		_ = jd.backgroundGroup.Wait()
	}
}

// TearDown stops the background goroutines,
//
//	waits until they finish and closes the database.
func (jd *Handle) TearDown() {
	jd.Stop()
	jd.Close()
}

// Close closes the database connection.
//
//	Stop should be called before Close.
//
//	Noop if the connection pool is shared with the handle.
func (jd *Handle) Close() {
	if !jd.sharedConnectionPool {
		if err := jd.dbHandle.Close(); err != nil {
			jd.logger.Errorn("error closing db connection", obskit.Error(err))
		}
	}
}

func (jd *Handle) startDropDSLoop(ctx context.Context) {
	jd.backgroundGroup.Go(crash.Wrapper(func() error {
		err := jd.dropDSLoop(ctx)
		if err != nil && ctx.Err() == nil {
			panic(fmt.Errorf("dropDSLoop for prefix %q: %w", jd.tablePrefix, err))
		}
		return nil
	}))
}

func (jd *Handle) dropDSLoop(ctx context.Context) error {
	nextDropDSEntry := func(ctx context.Context) (dropDSEntry, bool) {
		for {
			jd.dropDSListLock.RLock()
			if len(jd.dropDSList) > 0 {
				entry := jd.dropDSList[0]
				jd.dropDSListLock.RUnlock()
				return entry, true
			}
			jd.dropDSListLock.RUnlock()

			select {
			case <-ctx.Done():
				return dropDSEntry{}, false
			case <-jd.dropNotify:
			}
		}
	}
	for {
		entry, ok := nextDropDSEntry(ctx)
		if !ok {
			return nil
		}
		// Wait until all operations using this dataset are done.
		// This ensures that we don't drop a dataset which is currently being read.
		drained, err := jd.dsList.wait(entry.version)
		if err != nil {
			return fmt.Errorf("wait for dsList drain: %w", err)
		}
		select {
		case <-drained:
		case <-ctx.Done():
			return nil
		}
		// drop the dataset
		if err := jd.dropDSWithCtx(ctx, entry.ds); err != nil {
			return fmt.Errorf("dropDSWithCtx: %w", err)
		}
		// update the lists and cache
		if err := jd.dsListLock.WithLockInCtx(ctx, func(l lock.LockToken) error {
			jd.dropDSListLock.Lock()
			defer jd.dropDSListLock.Unlock()
			// Remove the entry from dropDSList
			jd.dropDSList = lo.Filter(jd.dropDSList, func(e dropDSEntry, _ int) bool {
				return e.version != entry.version || e.ds.Index != entry.ds.Index
			})
			// delete the entry from dsRangeFuncMap
			delete(jd.dsRangeFuncMap, entry.ds.Index)
			// Invalidate the distinctValuesCache for the dropped dataset
			jd.distinctValuesCache.RemoveDataset(entry.ds.JobTable)
			// If there are more datasets to drop, notify the dropDSLoop to check the next one
			if len(jd.dropDSList) > 0 {
				jd.dropNotifyPing()
			}
			return nil
		}); err != nil {
			return fmt.Errorf("removeDropDSEntry: %w", err)
		}
	}
}

/*
JobsDB uses separate locks for dataset-list publication and compaction.

Store only needs a snapshot of the latest dataset, so it reads dsListLock and
then writes to that dataset. If the snapshot is stale because another worker
rolled the dataset over, the store path retries after refreshing the list.

Reads and status updates need a dataset snapshot that stays valid while they
query or route statuses. They take the compaction read lock and acquire a
versioned dataset-list snapshot. Dataset drops wait for older snapshot versions
to drain before physically dropping tables.

Legacy compaction takes the compaction write lock while moving jobs. The
non-blocking compaction path avoids that write lock, fences source status
tables with read-only triggers, publishes the refreshed dataset list near commit,
and queues old datasets for asynchronous drop.
*/
func (jd *Handle) addNewDSLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-jd.TriggerAddNewDS():
		}
		var dsListLock lock.LockToken
		var releaseDsListLock chan<- lock.LockToken
		addNewDS := func() error {
			ctx, cancel := context.WithTimeout(ctx, jd.conf.addNewDSTimeout.Load())
			defer cancel()
			defer func() {
				if releaseDsListLock != nil && dsListLock != nil {
					releaseDsListLock <- dsListLock
				}
			}()
			// Adding a new DS only creates a new DS & updates the cache. It doesn't move any data so we only take the list lock.
			// start a transaction
			err := jd.withMaintenanceTx(ctx, func(tx *Tx) error {
				return jd.withDistributedSharedLock(ctx, tx, "schema_migrate", func() error { // cannot run while schema migration is running
					return jd.withDistributedLock(ctx, tx, "add_ds", func() error { // only one add_ds can run at a time
						var err error
						// refresh ds list
						var dsList []dataSetT
						var nextDSIdx string
						// make sure we are operating on the latest version of the list
						dsList, err = getDSList(jd, tx, jd.tablePrefix)
						if err != nil {
							return fmt.Errorf("getDSList: %w", err)
						}
						latestDS := dsList[len(dsList)-1]
						full, err := jd.checkIfFullDSInTx(tx, latestDS)
						if err != nil {
							return fmt.Errorf("checkIfFullDSInTx: %w", err)
						}
						// checkIfFullDS is true for last DS in the list
						if full {
							// We acquire the list lock only after we have acquired the advisory lock.
							// We will release the list lock after the transaction ends, that's why we need to use an async lock
							dsListLock, releaseDsListLock, err = jd.dsListLock.AsyncLockWithCtx(ctx)
							if err != nil {
								return fmt.Errorf("acquiring dsListLock: %w", err)
							}
							jd.logger.Infon("[[ addNewDSLoop ]]: Acquired lock",
								logger.NewStringField("ds", latestDS.String()),
								logger.NewStringField("jobsdb", jd.tablePrefix))
							if _, err = tx.ExecContext(ctx, fmt.Sprintf(`LOCK TABLE %q IN EXCLUSIVE MODE;`, latestDS.JobTable)); err != nil {
								return fmt.Errorf("error locking table %s: %w", latestDS.JobTable, err)
							}

							nextDSIdx = jd.doComputeNewIdxForAppend(dsList)
							jd.logger.Infon("[[ addNewDSLoop ]]: NewDS", logger.NewStringField("tablePrefix", jd.tablePrefix))
							if err = jd.addNewDSInTx(ctx, tx, dsListLock, dsList, newDataSet(jd.tablePrefix, nextDSIdx)); err != nil {
								return fmt.Errorf("error adding new DS: %w", err)
							}

							// previous DS should become read only
							if err = setReadonlyDsInTx(ctx, tx, latestDS); err != nil {
								return fmt.Errorf("error making dataset read only: %w", err)
							}
						} else {
							// maybe another node added a new DS that we need to make visible to us
							if err := jd.refreshDSListWithDB(ctx, tx); err != nil {
								return fmt.Errorf("refreshDSList: %w", err)
							}
						}
						return nil
					})
				})
			})
			if err != nil {
				return fmt.Errorf("addNewDSLoop: %w", err)
			}
			// to get the updated DS list in the cache after createDS transaction has been committed.
			if dsListLock != nil {
				if err = jd.doRefreshDSRangeList(dsListLock); err != nil {
					return fmt.Errorf("refreshDSRangeList: %w", err)
				}
			}
			return nil
		}
		if err := addNewDS(); err != nil {
			if !jd.conf.skipMaintenanceError && ctx.Err() == nil {
				panic(fmt.Errorf("adding new ds for %q: %w", jd.tablePrefix, err))
			}
			jd.logger.Errorn("addNewDSLoop error", obskit.Error(err))
		}
	}
}

func (jd *Handle) refreshDSListLoop(ctx context.Context) {
	for {
		select {
		case <-jd.TriggerRefreshDS():
		case <-ctx.Done():
			return
		}
		timeoutCtx, cancel := context.WithTimeout(ctx, jd.conf.refreshDSTimeout.Load())
		if err := jd.RefreshDSList(timeoutCtx); err != nil {
			cancel()
			if !jd.conf.skipMaintenanceError && ctx.Err() == nil {
				panic(err)
			}
			jd.logger.Errorn("refreshDSListLoop error", obskit.Error(err))
		}
		cancel()
	}
}

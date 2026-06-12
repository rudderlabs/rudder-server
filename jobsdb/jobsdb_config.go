package jobsdb

import (
	"time"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
)

func (jd *Handle) loadConfig() {
	// maxTableSizeInMB: Maximum Table size in MB
	jd.conf.maxTableSize = jd.config.GetReloadableInt64Var(300, 1000000, jd.configKeys("maxTableSizeInMB")...)
	jd.conf.cacheExpiration = jd.config.GetReloadableDurationVar(120, time.Minute, jd.configKeys("cacheExpiration")...)
	// addNewDSLoopSleepDuration: How often is the loop (which checks for adding new DS) run
	jd.conf.addNewDSLoopSleepDuration = jd.config.GetReloadableDurationVar(5, time.Second, jd.configKeys("addNewDSLoopSleepDuration")...)
	// refreshDSListLoopSleepDuration: How often is the loop (which refreshes DSList) run
	jd.conf.refreshDSListLoopSleepDuration = jd.config.GetReloadableDurationVar(10, time.Second, jd.configKeys("refreshDSListLoopSleepDuration")...)

	jd.conf.enableWriterQueue = jd.config.GetBoolVar(true, jd.configKeys("enableWriterQueue")...)
	jd.conf.enableReaderQueue = jd.config.GetBoolVar(true, jd.configKeys("enableReaderQueue")...)
	jd.conf.maxWriters = jd.config.GetIntVar(3, 1, jd.configKeys("maxWriters")...)
	jd.conf.maxReaders = jd.config.GetIntVar(6, 1, jd.configKeys("maxReaders")...)
	jd.conf.maxOpenConnections = jd.config.GetIntVar(20, 1, jd.configKeys("maxOpenConnections")...)
	jd.conf.analyzeThreshold = jd.config.GetReloadableIntVar(30000, 1, jd.configKeys("analyzeThreshold")...)
	jd.conf.minDSRetentionPeriod = jd.config.GetReloadableDurationVar(0, time.Minute, jd.configKeys("minDSRetention")...)
	jd.conf.maxDSRetentionPeriod = jd.config.GetReloadableDurationVar(90, time.Minute, jd.configKeys("maxDSRetention")...)
	jd.conf.refreshDSTimeout = jd.config.GetReloadableDurationVar(10, time.Minute, jd.configKeys("refreshDS.timeout")...)
	jd.conf.addNewDSTimeout = jd.config.GetReloadableDurationVar(5, time.Minute, jd.configKeys("addNewDS.timeout")...)

	// compactionConfig.
	// Compaction-named keys are the primary names; migrate-named keys are
	// legacy aliases. Scoped keys are checked before global keys.

	// compactionLoopSleepDuration: How often is the loop (which checks for compacting DS) run
	jd.conf.compaction.compactionLoopSleepDuration = jd.config.GetReloadableDurationVar(30, time.Second, jd.configKeys("compactionLoopSleepDuration", "migrateDSLoopSleepDuration", "migrateDSLoopSleepDurationInS")...)
	jd.conf.compaction.compactionTimeout = jd.config.GetReloadableDurationVar(10, time.Minute, jd.configKeys("compactionTimeout", "migrateDS.timeout")...)
	// jobStatusCompactionThres: A DS is compacted if the job_status exceeds this (* no_of_jobs)
	jd.conf.compaction.jobStatusCompactionThres = jd.config.GetReloadableFloat64Var(3, jd.configKeys("jobStatusCompactionThres", "jobStatusMigrateThreshold")...)
	// jobMinRowsLeftCompactionThreshold: A DS with a low number of pending rows should be eligible for compaction if the number of pending rows are
	// less than jobMinRowsLeftCompactionThreshold percent of maxDSSize (e.g. if jobMinRowsLeftCompactionThreshold is 0.5
	// then DSs that have less than 50% of maxDSSize pending rows are eligible for compaction)
	jd.conf.compaction.jobMinRowsLeftCompactionThreshold = jd.config.GetReloadableFloat64Var(0.6, jd.configKeys("jobMinRowsLeftCompactionThreshold", "jobMinRowsLeftMigrateThreshold")...)
	// compactionMinDSAge: a partially-processed DS (one that needs pairing) is not eligible for compaction
	// until it is at least this old, so that freshly-created compaction destinations are not compacted again right away
	jd.conf.compaction.compactionMinDSAge = jd.config.GetReloadableDurationVar(2, time.Minute, jd.configKeys("compactionMinDSAge")...)
	// maxCompactOnce: Maximum number of DSs that are compacted together into one destination
	jd.conf.compaction.maxCompactOnce = jd.config.GetReloadableIntVar(10, 1, jd.configKeys("maxCompactOnce", "maxMigrateOnce")...)
	// maxCompactDSProbe: Maximum number of DSs that are checked from left to right if they are eligible for compaction
	jd.conf.compaction.maxCompactDSProbe = jd.config.GetReloadableIntVar(10, 1, jd.configKeys("maxCompactDSProbe", "maxMigrateDSProbe")...)
	jd.conf.compaction.vacuumFullStatusTableThreshold = jd.config.GetReloadableInt64Var(500*bytesize.MB, 1, jd.configKeys("vacuumFullStatusTableThreshold")...)
	jd.conf.compaction.vacuumAnalyzeStatusTableThreshold = jd.config.GetReloadableInt64Var(30000, 1, jd.configKeys("vacuumAnalyzeStatusTableThreshold")...)
	jd.conf.compaction.nonBlockingCompletedDSDrop = jd.config.GetReloadableBoolVar(false, jd.configKeys("nonBlockingCompletedDSDrop")...)
	jd.conf.compaction.nonBlockingCompaction = jd.config.GetReloadableBoolVar(false, jd.configKeys("nonBlockingCompaction")...)
	jd.conf.compaction.compactionDeferStatusLock = jd.config.GetReloadableBoolVar(false, jd.configKeys("compactionDeferStatusLock")...)
	jd.conf.compaction.getJobsRetryOnCompaction = jd.config.GetReloadableBoolVar(true, jd.configKeys("getJobsRetryOnCompaction")...)
	if jd.conf.multiConsumer { // if multiConsumer is enabled, we skip status compaction by default
		jd.conf.compaction.skipStatusCompaction = jd.config.GetReloadableBoolVar(true, jd.configKeys("skipMultiConsumerStatusCompaction", "skipStatusCompaction")...)
	} else {
		jd.conf.compaction.skipStatusCompaction = jd.config.GetReloadableBoolVar(false, jd.configKeys("skipStatusCompaction")...)
	}

	// maxDSSize: Maximum size of a DS. The process which adds new DS runs in the background
	// (every few seconds) so a DS may go beyond this size
	// passing `maxDSSize` by reference, so it can be hot reloaded
	jd.conf.MaxDSSize = jd.config.GetReloadableIntVar(100000, 1, jd.configKeys("maxDSSize")...)

	// starting with false as default since initial set of migrated jobs will not have partitionID set
	jd.conf.warnOnStatusMissingPartitionID = jd.config.GetReloadableBoolVar(false, jd.configKeys("warnOnStatusMissingPartitionID")...)

	// Default false: snapshot lastDS and release the dsList read lock before running the store callback,
	// so long-running stores don't block dsList writers. Flip to true to revert to holding the lock for the whole callback.
	jd.conf.holdDSListLockDuringStore = jd.config.GetReloadableBoolVar(false, jd.configKeys("holdDSListLockDuringStore")...)
	jd.conf.staleDSListMaxRetries = jd.config.GetReloadableIntVar(3, 1, jd.configKeys("staleDSListMaxRetries")...)

	// when true, the per-state noResultsCache optimization is enabled: stateFilters are narrowed
	// against the cache before querying, and (!ok && !limitsReached) is used as a commit predicate.
	jd.conf.noResultsCacheStateOptimization = jd.config.GetReloadableBoolVar(false, jd.configKeys("noResultsCacheStateOptimization")...)
	jd.conf.getJobsUseLateralJoin = jd.config.GetReloadableBoolVar(true, jd.configKeys("getJobsUseLateralJoin")...)
	jd.conf.disallowMultiConsumerDowngrade = jd.config.GetBoolVar(false, jd.configKeys("disallowMultiConsumerDowngrade")...)

	if jd.TriggerAddNewDS == nil {
		jd.TriggerAddNewDS = func() <-chan time.Time {
			return time.After(jd.conf.addNewDSLoopSleepDuration.Load())
		}
	}

	if jd.TriggerCompaction == nil {
		jd.TriggerCompaction = func() <-chan time.Time {
			return time.After(jd.conf.compaction.compactionLoopSleepDuration.Load())
		}
	}

	if jd.TriggerRefreshDS == nil {
		jd.TriggerRefreshDS = func() <-chan time.Time {
			return time.After(jd.conf.refreshDSListLoopSleepDuration.Load())
		}
	}

	if jd.conf.jobMaxAge == nil {
		jd.conf.jobMaxAge = jd.config.GetReloadableDurationVar(720, time.Hour, jd.configKeys("jobMaxAge")...)
	}
}

func (jd *Handle) configKeys(key string, additionalKeys ...string) []string {
	res := make([]string, 0, 2+2*len(additionalKeys))
	res = append(res, "JobsDB."+jd.tablePrefix+"."+key)
	for _, additionalKey := range additionalKeys {
		res = append(res, "JobsDB."+jd.tablePrefix+"."+additionalKey)
	}
	res = append(res, "JobsDB."+key)
	for _, additionalKey := range additionalKeys {
		res = append(res, "JobsDB."+additionalKey)
	}
	return res
}

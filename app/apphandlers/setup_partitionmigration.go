package apphandlers

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/cluster/migrator/etcdclient"
	migrator "github.com/rudderlabs/rudder-server/cluster/migrator/gateway"
	"github.com/rudderlabs/rudder-server/cluster/migrator/processor"
	"github.com/rudderlabs/rudder-server/cluster/migrator/processor/sourcenode"
	"github.com/rudderlabs/rudder-server/cluster/migrator/processor/targetnode"
	"github.com/rudderlabs/rudder-server/cluster/partitionbuffer"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

type PartitionMigrator interface {
	Start() error
	Stop()
}

// ProcessorPartitionMigratorSetup holds the result of setting up the processor partition migrator.
type ProcessorPartitionMigratorSetup struct {
	PartitionMigrator PartitionMigrator
	GwDB              jobsdb.JobsDB
	RtDB              jobsdb.JobsDB
	BrtDB             jobsdb.JobsDB
	Finally           func()
}

// setupProcessorPartitionMigrator sets up the partition migrator for processor nodes (app running in processor or embedded mode)
func setupProcessorPartitionMigrator(ctx context.Context,
	shutdownFn func(), // called to initiate shutdown of the app
	dbPool *sql.DB, // database handle
	priorityPool *sql.DB, // priority database handle
	config *config.Config, stats stats.Stats,
	gwRODB, gwWODB, // gateway reader and writer jobsDB handles. if gwWODB is nil, gwRODB is used for reading and a new writer gw DB is created internally
	rtRWDB, brtRWDB jobsdb.JobsDB,
	etcdClientProvider func() (etcdclient.Client, error),
) (ProcessorPartitionMigratorSetup, error) {
	if !config.GetBool("PartitionMigration.enabled", false) {
		finally := func() {}
		if gwWODB == nil {
			// caller expects to get reader gw db back if writer is nil
			return ProcessorPartitionMigratorSetup{
				PartitionMigrator: &noOpPartitionMigrator{},
				GwDB:              gwRODB,
				RtDB:              rtRWDB,
				BrtDB:             brtRWDB,
				Finally:           finally,
			}, nil
		}
		// caller expects to get writer gw db back if writer is not nil
		return ProcessorPartitionMigratorSetup{
			PartitionMigrator: &noOpPartitionMigrator{},
			GwDB:              gwWODB,
			RtDB:              rtRWDB,
			BrtDB:             brtRWDB,
			Finally:           finally,
		}, nil
	}
	ppmSetup := ProcessorPartitionMigratorSetup{
		Finally: func() {},
	}

	log := logger.NewLogger().Child("partitionmigration")
	partitionCount := config.GetIntVar(0, 1, "JobsDB.partitionCount")
	if partitionCount == 0 {
		return ppmSetup, fmt.Errorf("partition migrator needs partition count > 0")
	}
	bufferFlushBatchSize := config.GetReloadableIntVar(20000, 1, "PartitionMigration.bufferFlushBatchSize")
	bufferFlushPayloadSize := config.GetReloadableInt64Var(500, bytesize.MB, "PartitionMigration.bufferFlushPayloadSize")
	bufferFlushMoveTimeout := config.GetReloadableDurationVar(30, time.Minute, "PartitionMigration.bufferFlushMoveTimeout")
	bufferWatchdogInterval := config.GetReloadableDurationVar(5, time.Minute, "PartitionMigration.bufferWatchdogInterval")

	// setup partition buffer for gateway jobsDB
	var gwSetupOpt partitionbuffer.Opt
	if gwWODB != nil {
		// we have separate reader and writer gw DBs, writer is externally managed
		// and we are going to create a single buffer using both
		gwBuffRWHandle := jobsdb.NewForReadWrite(
			"gw_buf",
			jobsdb.WithClearDB(false),
			jobsdb.WithDSLimit(config.GetReloadableIntVar(0, 1, "JobsDB.gw_buf.dsLimit", "JobsDB.dsLimit")),
			jobsdb.WithSkipMaintenanceErr(config.GetBoolVar(true, "JobsDB.gw_buf.skipMaintenanceError", "JobsDB.buff.skipMaintenanceError", "JobsDB.skipMaintenanceError")),
			jobsdb.WithStats(stats),
			jobsdb.WithDBHandle(dbPool),
			jobsdb.WithPriorityPoolDB(priorityPool),
		)
		if err := gwBuffRWHandle.Start(); err != nil {
			return ppmSetup, fmt.Errorf("starting gw buffer jobsdb handle: %w", err)
		}
		ppmSetup.Finally = func() {
			gwBuffRWHandle.Stop()
		}
		gwSetupOpt = partitionbuffer.WithSeparateReaderAndWriterPrimaryJobsDBs(gwRODB, gwWODB, gwBuffRWHandle)
	} else {
		// we have only a reader gw DB, so we create a buffer with reader and
		// a new writer gw DB that we create here so that it can be used for flushing buffered jobs
		gwWODB = jobsdb.NewForWrite(
			"gw",
			jobsdb.WithClearDB(false),
			jobsdb.WithStats(stats),
			jobsdb.WithDBHandle(dbPool),
			jobsdb.WithPriorityPoolDB(priorityPool),
			jobsdb.WithNumPartitions(partitionCount),
		)
		gwBuffROHandle := jobsdb.NewForRead(
			"gw_buf",
			jobsdb.WithDSLimit(config.GetReloadableIntVar(0, 1, "JobsDB.gw_buf.dsLimit", "JobsDB.dsLimit")),
			jobsdb.WithSkipMaintenanceErr(config.GetBoolVar(true, "JobsDB.gw_buf.skipMaintenanceError", "JobsDB.buff.skipMaintenanceError", "JobsDB.skipMaintenanceError")),
			jobsdb.WithStats(stats),
			jobsdb.WithDBHandle(dbPool),
			jobsdb.WithPriorityPoolDB(priorityPool),
		)
		gwSetupOpt = partitionbuffer.WithReaderOnlyAndFlushJobsDBs(gwRODB, gwBuffROHandle, gwWODB)
	}

	gwPartitionBuffer, err := partitionbuffer.NewJobsDBPartitionBuffer(ctx,
		gwSetupOpt,
		partitionbuffer.WithNumPartitions(partitionCount),
		partitionbuffer.WithStats(stats),
		partitionbuffer.WithLogger(log),
		partitionbuffer.WithFlushBatchSize(bufferFlushBatchSize),
		partitionbuffer.WithFlushPayloadSize(bufferFlushPayloadSize),
		partitionbuffer.WithFlushMoveTimeout(bufferFlushMoveTimeout),
		partitionbuffer.WithWatchdogInterval(bufferWatchdogInterval),
	)
	if err != nil {
		return ppmSetup, fmt.Errorf("creating gw partition buffer: %w", err)
	}
	ppmSetup.GwDB = gwPartitionBuffer

	// setup partition buffer for router jobsDB
	rtBuffRWHandle := jobsdb.NewForReadWrite(
		"rt_buf",
		jobsdb.WithClearDB(false),
		jobsdb.WithDSLimit(config.GetReloadableIntVar(0, 1, "JobsDB.rt_buff.dsLimit", "JobsDB.dsLimit")),
		jobsdb.WithSkipMaintenanceErr(config.GetBoolVar(true, "JobsDB.rt_buff.skipMaintenanceError", "JobsDB.buff.skipMaintenanceError", "JobsDB.skipMaintenanceError")),
		jobsdb.WithStats(stats),
		jobsdb.WithDBHandle(dbPool),
		jobsdb.WithPriorityPoolDB(priorityPool),
	)
	rtPartitionBuffer, err := partitionbuffer.NewJobsDBPartitionBuffer(ctx,
		partitionbuffer.WithReadWriteJobsDBs(rtRWDB, rtBuffRWHandle),
		partitionbuffer.WithNumPartitions(partitionCount),
		partitionbuffer.WithStats(stats),
		partitionbuffer.WithLogger(log),
		partitionbuffer.WithFlushBatchSize(bufferFlushBatchSize),
		partitionbuffer.WithFlushPayloadSize(bufferFlushPayloadSize),
		partitionbuffer.WithFlushMoveTimeout(bufferFlushMoveTimeout),
		partitionbuffer.WithWatchdogInterval(bufferWatchdogInterval),
	)
	if err != nil {
		return ppmSetup, fmt.Errorf("creating rt partition buffer: %w", err)
	}
	ppmSetup.RtDB = rtPartitionBuffer

	// setup partition buffer for batchrouter jobsDB
	brtBuffRWHandle := jobsdb.NewForReadWrite(
		"batch_rt_buf",
		jobsdb.WithClearDB(false),
		jobsdb.WithDSLimit(config.GetReloadableIntVar(0, 1, "JobsDB.batch_rt_buff.dsLimit", "JobsDB.dsLimit")),
		jobsdb.WithSkipMaintenanceErr(config.GetBoolVar(true, "JobsDB.batch_rt_buff.skipMaintenanceError", "JobsDB.buff.skipMaintenanceError", "JobsDB.skipMaintenanceError")),
		jobsdb.WithStats(stats),
		jobsdb.WithDBHandle(dbPool),
		jobsdb.WithPriorityPoolDB(priorityPool),
	)
	brtPartitionBuffer, err := partitionbuffer.NewJobsDBPartitionBuffer(ctx,
		partitionbuffer.WithReadWriteJobsDBs(brtRWDB, brtBuffRWHandle),
		partitionbuffer.WithNumPartitions(partitionCount),
		partitionbuffer.WithStats(stats),
		partitionbuffer.WithLogger(log),
		partitionbuffer.WithFlushBatchSize(bufferFlushBatchSize),
		partitionbuffer.WithFlushPayloadSize(bufferFlushPayloadSize),
		partitionbuffer.WithFlushMoveTimeout(bufferFlushMoveTimeout),
		partitionbuffer.WithWatchdogInterval(bufferWatchdogInterval),
	)
	if err != nil {
		return ppmSetup, fmt.Errorf("creating batch rt partition buffer: %w", err)
	}
	ppmSetup.BrtDB = brtPartitionBuffer

	// setup partition migrator
	etcdClient, err := etcdClientProvider()
	if err != nil {
		return ppmSetup, fmt.Errorf("getting etcd client: %w", err)
	}
	nodeIndex := config.GetIntVar(-1, 1, "PROCESSOR_INDEX")
	if nodeIndex < 0 {
		return ppmSetup, fmt.Errorf("got invalid node index from config: %d", nodeIndex)
	}
	nodeName := config.GetStringVar("", "INSTANCE_ID")
	if nodeName == "" {
		return ppmSetup, fmt.Errorf("got empty node name from config")
	}

	targetURLProvider, err := func() (func(targetNodeIndex int) (string, error), error) {
		processorNodeHostPattern := config.GetStringVar("", "PROCESSOR_NODE_HOST_PATTERN")
		if processorNodeHostPattern == "" {
			return nil, fmt.Errorf("empty PROCESSOR_NODE_HOST_PATTERN config")
		}
		nodeIndexPlaceholder := "{index}"
		if !strings.Contains(processorNodeHostPattern, nodeIndexPlaceholder) && config.GetBoolVar(true, "PartitionMigration.failOnInvalidNodeHostPattern") {
			return nil, fmt.Errorf("invalid PROCESSOR_NODE_HOST_PATTERN config: %s", processorNodeHostPattern)
		}
		port := config.GetIntVar(8088, 1,
			"PartitionMigration.Grpc.Server.TargetPort", // this is for tests where we cannot start multiple grpc servers on the same port
			"PartitionMigration.Grpc.Server.Port",
		)
		return func(targetNodeIndex int) (string, error) {
			return strings.Replace(processorNodeHostPattern, nodeIndexPlaceholder, strconv.Itoa(targetNodeIndex), 1) + ":" + strconv.Itoa(port), nil
		}, nil
	}()
	if err != nil {
		return ppmSetup, fmt.Errorf("creating target URL provider: %w", err)
	}

	sourceMigrator, err := sourcenode.NewMigratorBuilder(nodeIndex, nodeName).
		WithConfig(config).
		WithStats(stats).
		WithLogger(log).
		WithShutdown(shutdownFn).
		WithEtcdClient(etcdClient).
		WithTargetURLProvider(targetURLProvider).
		WithReaderJobsDBs([]jobsdb.JobsDB{gwRODB, rtRWDB, brtRWDB}).
		Build()
	if err != nil {
		return ppmSetup, fmt.Errorf("creating source node migrator: %w", err)
	}

	targetMigrator, err := targetnode.NewMigratorBuilder(nodeIndex, nodeName).
		WithConfig(config).
		WithStats(stats).
		WithLogger(log).
		WithEtcdClient(etcdClient).
		WithBufferedJobsDBs([][]partitionbuffer.JobsDBPartitionBuffer{
			{rtPartitionBuffer, brtPartitionBuffer},
			{gwPartitionBuffer},
		}).
		WithUnbufferedJobsDBs([]jobsdb.JobsDB{rtRWDB, brtRWDB, gwWODB}).
		Build()
	if err != nil {
		return ppmSetup, fmt.Errorf("creating target node migrator: %w", err)
	}
	ppm, err := processor.NewProcessorPartitionMigratorBuilder(nodeIndex, nodeName).
		WithConfig(config).
		WithStats(stats).
		WithLogger(log).
		WithEtcdClient(etcdClient).
		WithSourceMigrator(sourceMigrator).
		WithTargetMigrator(targetMigrator).
		Build()
	if err != nil {
		return ppmSetup, fmt.Errorf("creating processor partition migrator: %w", err)
	}
	ppmSetup.PartitionMigrator = ppm
	return ppmSetup, nil
}

func setupGatewayPartitionMigrator(ctx context.Context,
	dbPool *sql.DB, // database handle pool
	config *config.Config, stats stats.Stats,
	gwWODB jobsdb.JobsDB,
	etcdClientProvider func() (etcdclient.Client, error),
) (partitionMigrator PartitionMigrator, gwDB jobsdb.JobsDB, err error) {
	if !config.GetBool("PartitionMigration.enabled", false) {
		return &noOpPartitionMigrator{}, gwWODB, nil
	}
	partitionCount := config.GetIntVar(0, 1, "JobsDB.partitionCount")
	if partitionCount == 0 {
		return nil, nil, fmt.Errorf("partition migrator needs partition count > 0")
	}
	log := logger.NewLogger().Child("partitionmigration")

	gwBuffWOHandle := jobsdb.NewForWrite(
		"gw_buf",
		jobsdb.WithClearDB(false),
		jobsdb.WithSkipMaintenanceErr(config.GetBoolVar(true, "JobsDB.gw_buf.skipMaintenanceError", "JobsDB.buff.skipMaintenanceError", "JobsDB.skipMaintenanceError")),
		jobsdb.WithStats(stats),
		jobsdb.WithDBHandle(dbPool),
	)
	if err := gwBuffWOHandle.Start(); err != nil {
		return nil, nil, fmt.Errorf("starting gw buffer jobsdb handle: %w", err)
	}
	gwPartitionBuffer, err := partitionbuffer.NewJobsDBPartitionBuffer(ctx,
		partitionbuffer.WithWriterOnlyJobsDBs(gwWODB, gwBuffWOHandle),
		partitionbuffer.WithNumPartitions(partitionCount),
		partitionbuffer.WithStats(stats),
		partitionbuffer.WithLogger(log),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("creating partition buffer: %w", err)
	}

	// setup partition migrator
	etcdClient, err := etcdClientProvider()
	if err != nil {
		return nil, nil, fmt.Errorf("getting etcd client: %w", err)
	}
	nodeIndex := config.GetIntVar(-1, 1, "GATEWAY_INDEX")
	if nodeIndex < 0 {
		return nil, nil, fmt.Errorf("got invalid node index from config: %d", nodeIndex)
	}
	nodeName := config.GetStringVar("", "INSTANCE_ID")
	if nodeName == "" {
		return nil, nil, fmt.Errorf("got empty node name from config")
	}

	gpm, err := migrator.NewGatewayPartitionMigratorBuilder(nodeIndex, nodeName).
		WithConfig(config).
		WithStats(stats).
		WithLogger(log).
		WithEtcdClient(etcdClient).
		WithPartitionRefresher(gwPartitionBuffer).
		Build()
	if err != nil {
		return nil, nil, fmt.Errorf("creating gateway partition migrator: %w", err)
	}

	return gpm, gwPartitionBuffer, nil
}

type noOpPartitionMigrator struct{}

func (n *noOpPartitionMigrator) Start() error {
	return nil
}

func (n *noOpPartitionMigrator) Stop() {
}

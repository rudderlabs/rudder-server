package partitionmigration_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/etcd"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"
	etcdtypes "github.com/rudderlabs/rudder-schemas/go/cluster"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/testhelper/clustertest"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/rudderserver"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

// TestPartitionMigrationGatewayProcessorMode tests partition migration with servers running in gateway-processor mode.
//
// The test performs the following steps:
// 1. Starts 2 PostgreSQL containers.
// 2. Starts an etcd container.
// 3. Starts a transformer container.
// 4. Starts a test webhook to verify event order based on userId.
// 5. Starts a test backendconfig with 1 source connected to the webhook.
// 6. Starts 2 rudder-server node pairs (gw & proc) with some partitions.
// 6a. Processor nodes will be restarting periodically to verify migration idempotence.
// 7. Starts a test gateway proxy that listens for partition reload events and forwards requests to the relevant rudder-server gw node.
// 8. Starts a client goroutine that sends requests to the gateway proxy for all partitions at a prescribed rate.
// 9. Starts multiple migration controllers that each orchestrate one migration through etcd.
// 10. Waits for all migration controllers to complete their migrations and verifies that there were no errors.
// 11. Stops the client goroutine after a while.
// 12. Waits for all requests to complete.
// 13. Verifies that all requests were received successfully and in order.
func TestPartitionMigrationGatewayProcessorMode(t *testing.T) {
	const (
		namespace     = "namespace123"
		workspaceID   = "workspace123"
		sourceID      = "source123"
		destinationID = "destination123"
		writeKey      = "writekey123"

		numPartitions             = 4                // needs to be a power of 2 (e.g., 2, 4, 8, 16, ...)
		jobsPerPartitionPerSecond = 50               // number of jobs to send per partition per second from the gateway client
		restartProcessorEvery     = 10 * time.Second // how often to restart processor nodes while migration is ongoing
		readExcludeSleep          = 5 * time.Second  // sleep duration for read exclusion during migration
	)

	// distribute partitions across the 2 nodes equally
	initialMappings := map[int]int{}
	var node0Partitions, node1Partitions []int
	for i := range numPartitions {
		nodeIndex := i % 2
		switch nodeIndex {
		case 0:
			node0Partitions = append(node0Partitions, i)
		case 1:
			node1Partitions = append(node1Partitions, i)
		}
		initialMappings[i] = i % 2
	}
	// setup docker pool
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	// start 2 postgresql containers
	pg0, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	pg1, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	// start a transformer container
	tr, err := transformer.Setup(pool, t)
	require.NoError(t, err)

	// start an etcd container
	etcdResource, err := etcd.Setup(pool, t)
	require.NoError(t, err)

	// start a test webhook that will verify event order based on userId
	wh := newTestWebhook(t)
	t.Cleanup(wh.Close)

	// start a test backendconfig with 1 source connected to the webhook
	bc := backendconfigtest.NewBuilder().
		WithNamespace(namespace, backendconfigtest.NewConfigBuilder().
			WithWorkspaceID(workspaceID).
			WithSource(
				backendconfigtest.NewSourceBuilder().
					WithWorkspaceID(workspaceID).
					WithID(sourceID).
					WithWriteKey(writeKey).
					WithConnection(
						backendconfigtest.NewDestinationBuilder("WEBHOOK").
							WithID(destinationID).
							WithConfigOption("webhookMethod", "POST").
							WithConfigOption("webhookUrl", wh.URL).
							Build()).
					Build()).
			Build()).
		Build()

	gw0Port, err := kithelper.GetFreePort() // for gateway node 0
	require.NoError(t, err)
	proc0Port, err := kithelper.GetFreePort() // for processor node 0
	require.NoError(t, err)
	gw1Port, err := kithelper.GetFreePort() // for gateway node 1
	require.NoError(t, err)
	proc1Port, err := kithelper.GetFreePort() // for processor node 1
	require.NoError(t, err)
	grpc0Port, err := kithelper.GetFreePort() // for grpc server node 0
	require.NoError(t, err)
	grpc1Port, err := kithelper.GetFreePort() // for grpc server node 1
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)

	// start 2 rudder-server gw/proc pairs

	gwCommonEnv := map[string]string{
		"APP_TYPE": "gateway",

		"PartitionMigration.enabled": "true",

		"ETCD_HOSTS":            etcdResource.Hosts[0],
		"DEST_TRANSFORM_URL":    tr.TransformerURL,
		"WORKSPACE_NAMESPACE":   namespace,
		"RELEASE_NAME":          namespace,
		"CONFIG_BACKEND_URL":    bc.URL,
		"HOSTED_SERVICE_SECRET": "123",   // random one
		"enableStats":           "false", // disable stats for simplicity
		"RUDDER_TMPDIR":         t.TempDir(),
		"DEPLOYMENT_TYPE":       string(deployment.MultiTenantType), // we need etcd

		"AdminServer.enabled":                 "false", // disable admin server for simplicity
		"Profiler.Enabled":                    "false", // we don't need to specify a port if disabled
		"Gateway.allowPartialWriteWithErrors": "false", // not going through the lecacy gateway path

		// we want to create multiple datasets during the test and ensure that migration works correctly with ds limits as well
		"JobsDB.maxDSSize":                      "200",
		"JobsDB.addNewDSLoopSleepDuration":      "500ms",
		"JobsDB.dsLimit":                        "3",
		"JobsDB.refreshDSListLoopSleepDuration": "5s",
		"JobsDB.partitionCount":                 strconv.Itoa(numPartitions),
	}

	procCommonEnv := map[string]string{
		"APP_TYPE":                    "processor",
		"PROCESSOR_NODE_HOST_PATTERN": "proc-node-{index}.localhost",

		"PartitionMigration.enabled":                               "true",
		"PartitionMigration.failOnInvalidNodeHostPattern":          "false",
		"PartitionMigration.Processor.SourceNode.readExcludeSleep": strconv.Itoa(int(readExcludeSleep.Milliseconds())) + "ms", // sleep a bit less than the default one to speed up the test
		"PartitionMigration.SourceNode.inProgressPollSleep":        "1s",                                                      // poll faster for test speed
		"PartitionMigration.Executor.BatchSize":                    "100",                                                     // let migrations do multiple small batches
		"PartitionMigration.Executor.ChunkSize":                    "10",

		"ETCD_HOSTS":            etcdResource.Hosts[0],
		"DEST_TRANSFORM_URL":    tr.TransformerURL,
		"WORKSPACE_NAMESPACE":   namespace,
		"RELEASE_NAME":          namespace,
		"CONFIG_BACKEND_URL":    bc.URL,
		"HOSTED_SERVICE_SECRET": "123",   // random one
		"Warehouse.mode":        "off",   // turn off warehouse for simiplicity
		"enableStats":           "false", // disable stats for simplicity
		"RUDDER_TMPDIR":         t.TempDir(),
		"DEPLOYMENT_TYPE":       string(deployment.MultiTenantType), // we need etcd
		"archival.Enabled":      "false",                            // disable archival for simplicity

		// disable all live event debuggers for simplicity
		"DestinationDebugger.disableEventDeliveryStatusUploads":     "true",
		"SourceDebugger.disableEventUploads":                        "true",
		"TransformationDebugger.disableTransformationStatusUploads": "true",

		"AdminServer.enabled": "false", // disable admin server for simplicity
		"Profiler.Enabled":    "false", // we don't need to specify a port if disabled

		// we want to create multiple datasets during the test and ensure that migration works correctly with ds limits as well
		"JobsDB.maxDSSize":                      "200",
		"JobsDB.addNewDSLoopSleepDuration":      "500ms",
		"JobsDB.dsLimit":                        "3",
		"JobsDB.refreshDSListLoopSleepDuration": "5s",
		"JobsDB.partitionCount":                 strconv.Itoa(numPartitions),

		"Processor.pingerSleep":   "1s",
		"Processor.readLoopSleep": "1s",
		"Processor.maxLoopSleep":  "1s",

		"Router.readSleep":                         "1s",
		"Router.maxReadSleep":                      "5s",
		"Router.eventOrderKeyThreshold":            "0", // we need strict event ordering guarantees for this test
		"Router.noOfWorkers":                       strconv.Itoa(numPartitions),
		"Router.Network.IncludeInstanceIdInHeader": "true", // for debugging in case of receiving out-of-order events

	}
	rsBinaryPath := filepath.Join(t.TempDir(), "rudder-server-binary")
	rudderserver.BuildRudderServerBinary(t, "../../main.go", rsBinaryPath)
	gwNode0Name := "gw-node-0"
	procNode0Name := "proc-node-0"

	var skipProcessorRestart atomic.Bool

	rudderserver.StartRudderServer(t, ctx, g, gwNode0Name, rsBinaryPath, lo.Assign(gwCommonEnv, map[string]string{
		"GATEWAY_INDEX":   "0",
		"HOSTNAME":        gwNode0Name,
		"INSTANCE_ID":     gwNode0Name,
		"DB.host":         pg0.Host,
		"DB.port":         pg0.Port,
		"DB.user":         pg0.User,
		"DB.password":     pg0.Password,
		"DB.name":         pg0.Database,
		"Gateway.webPort": strconv.Itoa(gw0Port),
	}))
	health.WaitUntilReady(ctx, t,
		fmt.Sprintf("http://localhost:%d/health", gw0Port),
		20*time.Second,
		100*time.Millisecond,
		t.Name(),
	)
	restartingProcessorServer(t, ctx, g, procNode0Name, rsBinaryPath, lo.Assign(procCommonEnv, map[string]string{
		"PROCESSOR_INDEX":                     "0",
		"HOSTNAME":                            procNode0Name,
		"INSTANCE_ID":                         procNode0Name,
		"PartitionMigration.Grpc.Server.Port": strconv.Itoa(grpc0Port),
		"PartitionMigration.Grpc.Server.TargetPort": strconv.Itoa(grpc1Port),
		"DB.host":           pg0.Host,
		"DB.port":           pg0.Port,
		"DB.user":           pg0.User,
		"DB.password":       pg0.Password,
		"DB.name":           pg0.Database,
		"Processor.webPort": strconv.Itoa(proc0Port),
	}), &skipProcessorRestart, restartProcessorEvery)

	gwNode1Name := "gw-node-1"
	procNode1Name := "proc-node-1"

	rudderserver.StartRudderServer(t, ctx, g, gwNode1Name, rsBinaryPath, lo.Assign(gwCommonEnv, map[string]string{
		"GATEWAY_INDEX":   "1",
		"HOSTNAME":        gwNode1Name,
		"INSTANCE_ID":     gwNode1Name,
		"DB.host":         pg1.Host,
		"DB.port":         pg1.Port,
		"DB.user":         pg1.User,
		"DB.password":     pg1.Password,
		"DB.name":         pg1.Database,
		"Gateway.webPort": strconv.Itoa(gw1Port),
	}))
	health.WaitUntilReady(ctx, t,
		fmt.Sprintf("http://localhost:%d/health", gw1Port),
		20*time.Second,
		100*time.Millisecond,
		t.Name(),
	)
	restartingProcessorServer(t, ctx, g, procNode1Name, rsBinaryPath, lo.Assign(procCommonEnv, map[string]string{
		"PROCESSOR_INDEX":                     "1",
		"HOSTNAME":                            procNode1Name,
		"INSTANCE_ID":                         procNode1Name,
		"PartitionMigration.Grpc.Server.Port": strconv.Itoa(grpc1Port),
		"PartitionMigration.Grpc.Server.TargetPort": strconv.Itoa(grpc0Port),
		"DB.host":           pg1.Host,
		"DB.port":           pg1.Port,
		"DB.user":           pg1.User,
		"DB.password":       pg1.Password,
		"DB.name":           pg1.Database,
		"Processor.webPort": strconv.Itoa(proc1Port),
	}), &skipProcessorRestart, restartProcessorEvery)

	var backendUrls []string
	backendUrls = append(backendUrls, "http://127.0.0.1:"+strconv.Itoa(gw0Port))
	backendUrls = append(backendUrls, "http://127.0.0.1:"+strconv.Itoa(gw1Port))

	// start a routing proxy with 2 backends and relevant initial partition mappings
	routingProxy := clustertest.NewRoutingProxy(t, numPartitions, initialMappings, backendUrls...)
	defer routingProxy.Close()

	// start a gateway client that will send requests to the routing proxy
	gwClient := startGatewayClient(ctx, g, gatewayClientConfig{
		url:                       routingProxy.URL,
		writeKey:                  writeKey,
		numPartitions:             numPartitions,
		jobsPerPartitionPerSecond: jobsPerPartitionPerSecond,
	}, t)
	defer func() { _ = gwClient.Stop() }()

	// wait for some time to let events flow
	time.Sleep(10 * time.Second)

	if true { // quick way to enable/disable migration in the test for debugging
		start := time.Now()
		t.Logf("testscenario: starting migrations")
		// define a partition change listener that updates the routing proxy mappings
		partitionChangeListener := func(partitionID string, newNodeIndex int) {
			parts := strings.Split(partitionID, "-")
			partitionIdx, err := strconv.Atoi(parts[len(parts)-1])
			require.NoError(t, err, "partition ID should end with partition index")
			t.Logf("testscenario: starting routing partition %d to node %d", partitionIdx, newNodeIndex)
			routingProxy.UpdatePartitionMapping(partitionIdx, newNodeIndex)
		}
		migrationExecutorGroup, migrationExecutorCtx := errgroup.WithContext(ctx)

		// start a migration that swaps 2 first partitions between the nodes
		migrationExecutorGroup.Go(func() error {
			migrationID := "test-migration-0"
			migrationExecutor := clustertest.NewPartitionMigrationExecutor(
				namespace,
				etcdtypes.PartitionMigration{
					ID: migrationID,
					Jobs: []*etcdtypes.PartitionMigrationJobHeader{
						{ // move a partition from node 0 to node 1
							JobID:      migrationID + "-job-1",
							SourceNode: 0,
							TargetNode: 1,
							Partitions: []string{workspaceID + "-" + strconv.Itoa(node0Partitions[0])},
						},
						{ // move a partition from node 1 to node 0
							JobID:      migrationID + "-job-2",
							SourceNode: 1,
							TargetNode: 0,
							Partitions: []string{workspaceID + "-" + strconv.Itoa(node1Partitions[0])},
						},
					},
				},
				true,
				etcdResource.Client,
				partitionChangeListener,
				t,
			)
			migCtx, migCancel := context.WithTimeout(migrationExecutorCtx, 2*time.Minute)
			defer migCancel()
			start := time.Now()
			err = migrationExecutor.Run(migCtx)
			if err != nil {
				return fmt.Errorf("migration %q failed: %w", migrationID, err)
			}
			t.Logf("testscenario: migration %q completed in %s", migrationID, time.Since(start))
			return nil
		})

		if len(node0Partitions) > 1 {
			// start a migration that moves second partition from node0
			migrationExecutorGroup.Go(func() error {
				time.Sleep(2 * time.Second) // delay
				migrationID := "test-migration-1"
				migrationExecutor := clustertest.NewPartitionMigrationExecutor(
					namespace,
					etcdtypes.PartitionMigration{
						ID: migrationID,
						Jobs: []*etcdtypes.PartitionMigrationJobHeader{
							{ // move a partition from node 0 to node 1
								JobID:      migrationID + "-job-1",
								SourceNode: 0,
								TargetNode: 1,
								Partitions: []string{workspaceID + "-" + strconv.Itoa(node0Partitions[1])},
							},
						},
					},
					true,
					etcdResource.Client,
					partitionChangeListener,
					t,
				)
				migCtx, migCancel := context.WithTimeout(migrationExecutorCtx, 2*time.Minute)
				defer migCancel()
				start := time.Now()
				err = migrationExecutor.Run(migCtx)
				if err != nil {
					return fmt.Errorf("migration %q failed: %w", migrationID, err)
				}
				t.Logf("testscenario: migration %q completed in %s", migrationID, time.Since(start))
				return nil
			})
		}

		if len(node1Partitions) > 1 {
			// start a migration that moves second partition from node1
			migrationExecutorGroup.Go(func() error {
				time.Sleep(5 * time.Second) // delay
				migrationID := "test-migration-2"
				migrationExecutor := clustertest.NewPartitionMigrationExecutor(
					namespace,
					etcdtypes.PartitionMigration{
						ID: migrationID,
						Jobs: []*etcdtypes.PartitionMigrationJobHeader{
							{ // move a partition from node 1 to node 0
								JobID:      migrationID + "-job-1",
								SourceNode: 1,
								TargetNode: 0,
								Partitions: []string{workspaceID + "-" + strconv.Itoa(node1Partitions[1])},
							},
						},
					},
					true,
					etcdResource.Client,
					partitionChangeListener,
					t,
				)
				migCtx, migCancel := context.WithTimeout(migrationExecutorCtx, 2*time.Minute)
				defer migCancel()
				start := time.Now()
				err = migrationExecutor.Run(migCtx)
				if err != nil {
					return fmt.Errorf("migration %q failed: %w", migrationID, err)
				}
				t.Logf("testscenario: migration %q completed in %s", migrationID, time.Since(start))
				return nil
			})
		}
		require.NoError(t, migrationExecutorGroup.Wait(), "migrations should complete without error")
		t.Logf("testscenario: all migrations completed in %s", time.Since(start))
	} else {
		// just wait for a while with no migration
		time.Sleep(20 * time.Second)
	}

	// stop restarting processor nodes
	skipProcessorRestart.Store(true)

	// after migration is done, wait for some more time to let events flow on the other side as well
	time.Sleep(2 * time.Second)
	require.NoError(t, ctx.Err())

	// stop the gateway client
	err = gwClient.Stop()
	require.NoError(t, err, "stopping gateway client should not produce an error")

	// wait until all sent events are received by the webhook or context timeout occurs
	require.Eventually(t, func() bool {
		totalReceived := wh.totalEvents.Load()
		t.Logf("testscenario: total Sent: %d, total received: %d", gwClient.GetTotalSent(), totalReceived)
		require.LessOrEqualf(t, totalReceived, gwClient.GetTotalSent(), "received should be less or equal than sent")
		return totalReceived == gwClient.GetTotalSent()
	}, 2*time.Minute, 1*time.Second, "all sent events should be received by the webhook")

	require.NoError(t, ctx.Err(), "context should not have been cancelled or timed out")
	require.EqualValuesf(t, 0, wh.outOfOrderCount.Load(), "there should be no out of order events: %+v", wh.outOfOrderEvents)
	cancel() // cancel the main test context to stop all servers
	require.NoError(t, g.Wait(), "all goroutines should complete without error")
}

func restartingProcessorServer(t *testing.T, ctx context.Context, g *errgroup.Group, name, rsBinaryPath string, envVars map[string]string, skipRestart *atomic.Bool, restartEvery time.Duration) {
	runCtx, cancel := context.WithCancel(ctx)
	runGroup, runCtx := errgroup.WithContext(runCtx)
	rudderserver.StartRudderServer(t, runCtx, runGroup, name, rsBinaryPath, envVars)
	health.WaitUntilReady(ctx, t,
		fmt.Sprintf("http://localhost:%s/health", envVars["Processor.webPort"]),
		20*time.Second,
		100*time.Millisecond,
		t.Name(),
	)
	g.Go(func() error {
		var restarts int
		for {
			select {
			case <-ctx.Done():
				cancel() // stop the current server
				return runGroup.Wait()
			case <-time.After(restartEvery):
				if skipRestart.Load() {
					continue
				}
				restarts++
				t.Logf("restarting processor server %q", name)
				cancel() // stop the current server
				if err := runGroup.Wait(); err != nil {
					t.Logf("processor server %q exited with error: %v", name, err)
				} else {
					t.Logf("processor server %q exited cleanly", name)
				}
				// start a new one
				runCtx, cancel = context.WithCancel(ctx)
				runGroup, runCtx = errgroup.WithContext(runCtx)
				rudderserver.StartRudderServer(t, runCtx, runGroup, name, rsBinaryPath, envVars)
				health.WaitUntilReady(ctx, t,
					fmt.Sprintf("http://localhost:%s/health", envVars["Processor.webPort"]),
					20*time.Second,
					100*time.Millisecond,
					t.Name(),
				)
			}
		}
	})
}

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

	"github.com/rudderlabs/rudder-go-kit/partmap"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/etcd"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/rudo"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"
	"github.com/rudderlabs/rudder-go-kit/testhelper/localip"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rudoacker"
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
// 9. Starts a rudder-orchestrator and creates migrations.
// 10. Waits for the migrations to complete and verifies that there were no errors.
// 11. Stops the client goroutine after a while.
// 12. Waits for all requests to complete.
// 13. Verifies that all requests were received successfully and in order.
func TestPartitionMigrationGatewayProcessorMode(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		extraStressWorkspaces int           // number of extra workspace migrations to include (0 = normal mode)
		restartProcessorEvery time.Duration // how often to restart processor nodes while migration is ongoing
	}{
		{name: "normal", extraStressWorkspaces: 0, restartProcessorEvery: 25 * time.Second},
		{name: "stress_100_workspaces", extraStressWorkspaces: 100, restartProcessorEvery: 30 * time.Second},
		{name: "stress_1000_workspaces", extraStressWorkspaces: 1000, restartProcessorEvery: 35 * time.Second},
		{name: "stress_5000_workspaces", extraStressWorkspaces: 5000, restartProcessorEvery: 50 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testPartitionMigrationGatewayProcessorMode(t, tc.extraStressWorkspaces, tc.restartProcessorEvery)
		})
	}
}

func testPartitionMigrationGatewayProcessorMode(t *testing.T, extraStressWorkspaces int, restartProcessorEvery time.Duration) {
	const (
		namespace     = "namespace123"
		workspaceID   = "workspace123"
		sourceID      = "source123"
		destinationID = "destination123"
		writeKey      = "writekey123"

		numPartitions             = 4                // needs to be a power of 2 (e.g., 2, 4, 8, 16, ...)
		jobsPerPartitionPerSecond = 50               // number of jobs to send per partition per second from the gateway client
		readExcludeSleep          = 15 * time.Second // sleep duration for read exclusion during migration, must not be greater than restartProcessorEvery-5s
	)
	require.LessOrEqual(t, readExcludeSleep, restartProcessorEvery-5*time.Second,
		"readExcludeSleep must not be greater than restartProcessorEvery-5s")

	// distribute partitions across the 2 nodes equally
	initialMappings := map[partmap.PartitionIndex]partmap.NodeIndex{}
	var node0Partitions, node1Partitions []int
	for i := range numPartitions {
		nodeIndex := i % 2
		switch nodeIndex {
		case 0:
			node0Partitions = append(node0Partitions, i)
		case 1:
			node1Partitions = append(node1Partitions, i)
		}
		initialMappings[partmap.PartitionIndex(i)] = partmap.NodeIndex(i % 2)
	}
	// setup docker pool
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	localIp := localip.GetLocalIP()

	// start 2 postgresql containers
	pg0, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	pg1, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	// start a transformer container
	tr, err := transformer.Setup(pool, t)
	require.NoError(t, err)

	// start an etcd container
	etcdResource, err := etcd.Setup(pool, t, etcd.WithBindIP(localIp))
	require.NoError(t, err)

	// put mappings in etcd for the main workspace
	wpmh := clustertest.NewWorkspacePartitionMappingHandler(etcdResource.Client, numPartitions, namespace, workspaceID)
	err = wpmh.SetWorkspacePartitionMappings(t.Context(), initialMappings)
	require.NoError(t, err)

	// put mappings in etcd for extra stress workspaces (same partition mappings, no backend config needed)
	if extraStressWorkspaces > 0 {
		t.Logf("testscenario: setting up partition mappings for %d extra stress workspaces", extraStressWorkspaces)
		for i := range extraStressWorkspaces {
			stressWpmh := clustertest.NewWorkspacePartitionMappingHandler(etcdResource.Client, numPartitions, namespace, fmt.Sprintf("workspace-stress-%d", i))
			err = stressWpmh.SetWorkspacePartitionMappings(t.Context(), initialMappings)
			require.NoError(t, err)
		}
		t.Logf("testscenario: finished setting up partition mappings for %d extra stress workspaces", extraStressWorkspaces)
	}

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
	bc.URL = strings.Replace(bc.URL, "127.0.0.1", localIp, 1) // replace localhost with local IP for docker containers to access

	// start rudder-orchestrator that will run the migrations
	rudoResource, err := rudo.Setup(pool, t,
		rudo.WithBindIP(localIp),
		rudo.WithEtcdHosts(etcdResource.Hosts),
		rudo.WithGatewaySeparateService(true),
		rudo.WithReleaseName(namespace),
		rudo.WithPartitionCount(numPartitions),
		rudo.WithPollerBaseURL(bc.URL),
		rudo.WithWorkspaceNamespace(namespace),
		rudo.WithSrcRouterNodes([]string{"srcrouter"}),
		rudo.WithWorkspacePartitionGroups(1),
	)
	require.NoError(t, err)

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
		"LOG_LEVEL": "WARN",
		"APP_TYPE":  "gateway",

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
		"JobsDB.maxDSSize":                      "500",
		"JobsDB.addNewDSLoopSleepDuration":      "2s",
		"JobsDB.dsLimit":                        "3",
		"JobsDB.refreshDSListLoopSleepDuration": "5s",
		"JobsDB.partitionCount":                 strconv.Itoa(numPartitions),
	}

	procCommonEnv := map[string]string{
		"LOG_LEVEL":                   "WARN",
		"APP_TYPE":                    "processor",
		"PROCESSOR_NODE_HOST_PATTERN": "proc-node-{index}.localhost",

		"PartitionMigration.enabled":                               "true",
		"PartitionMigration.failOnInvalidNodeHostPattern":          "false",
		"PartitionMigration.Processor.SourceNode.readExcludeSleep": strconv.Itoa(int(readExcludeSleep.Milliseconds())) + "ms", // sleep a bit less than the default one to speed up the test
		"PartitionMigration.SourceNode.inProgressPollSleep":        "1s",                                                      // poll faster for test speed
		"PartitionMigration.Executor.BatchSize":                    "100",                                                     // let migrations do multiple small batches
		"PartitionMigration.Executor.ChunkSize":                    "10",
		"PartitionMigration.bufferFlushBatchSize":                  "100",
		"PartitionMigration.bufferFlushMoveConcurrency":            "2",
		"PartitionMigration.bufferWatchdogInterval":                "10s", // watch more frequently for surfacing potentials issues

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
		"JobsDB.maxDSSize":                      "500",
		"JobsDB.addNewDSLoopSleepDuration":      "2s",
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
		"Router.jobIterator.maxQueries":            "1",
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

	var srcRouterAcks []string
	err = rudoacker.NewSrcrouterAcker(ctx, g, etcdResource.Client, namespace, []string{"srcrouter"}).
		WithEventListener(func(key string, value etcdtypes.ReloadSrcRouterCommand) {
			// get new mappings and set them
			newMappings, err := wpmh.GetWorkspacePartitionMappings(ctx)
			require.NoError(t, err, "getting new partition mappings should not produce an error")
			t.Logf("testscenario: received reload command for key %s with new mappings: %+v", key, newMappings)
			routingProxy.SetPartitionMappings(newMappings)
		}).
		WithAckListener(func(ackKey string) {
			srcRouterAcks = append(srcRouterAcks, ackKey)
		}).
		Start()
	require.NoError(t, err)

	// buildMigrations wraps the real workspace migration with extra stress workspace migrations (if any)
	buildMigrations := func(realMigrations []rudo.Migration) []rudo.WorkspaceMigration {
		wms := []rudo.WorkspaceMigration{{
			WorkspaceID: workspaceID,
			Migrations:  realMigrations,
		}}
		for i := range extraStressWorkspaces {
			wms = append(wms, rudo.WorkspaceMigration{
				WorkspaceID: fmt.Sprintf("workspace-stress-%d", i),
				Migrations:  realMigrations, // same partition movements
			})
		}
		return wms
	}

	start := time.Now()
	t.Logf("testscenario: starting migrations (extraStressWorkspaces=%d)", extraStressWorkspaces)

	// create migration 0: swap first partitions between nodes
	_, err = rudoResource.CreateMigration(ctx, buildMigrations([]rudo.Migration{
		// move a partition from node 0 to node 1
		{Src: rudo.Src{ServerID: 0, PartitionIdxs: []int{node0Partitions[0]}}, Dst: rudo.Dst{ServerID: 1}},
		// move a partition from node 1 to node 0
		{Src: rudo.Src{ServerID: 1, PartitionIdxs: []int{node1Partitions[0]}}, Dst: rudo.Dst{ServerID: 0}},
	}))
	require.NoError(t, err)

	if len(node0Partitions) > 1 {
		// create migration 1: move second partition from node0 to node1
		_, err = rudoResource.CreateMigration(ctx, buildMigrations([]rudo.Migration{
			{Src: rudo.Src{ServerID: 0, PartitionIdxs: []int{node0Partitions[1]}}, Dst: rudo.Dst{ServerID: 1}},
		}))
		require.NoError(t, err)
	}

	if len(node1Partitions) > 1 {
		// create migration 2: move second partition from node1 to node0
		_, err = rudoResource.CreateMigration(ctx, buildMigrations([]rudo.Migration{
			{Src: rudo.Src{ServerID: 1, PartitionIdxs: []int{node1Partitions[1]}}, Dst: rudo.Dst{ServerID: 0}},
		}))
		require.NoError(t, err)
	}

	// wait for all migrations to complete
	require.Eventually(t, func() bool {
		migrations, err := rudoResource.ListMigrations(ctx)
		require.NoError(t, err)
		return len(migrations) == 0
	}, 5*time.Minute, 1*time.Second, "all migrations should complete within the timeout")
	end := time.Now()

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
		return totalReceived >= gwClient.GetTotalSent()
	}, 5*time.Minute, 1*time.Second, "all sent events should be received by the webhook")

	require.NoError(t, ctx.Err(), "context should not have been cancelled or timed out")
	require.EqualValuesf(t, 0, wh.outOfOrderCount.Load(), "there should be no out of order events: %+v", wh.outOfOrderEvents)
	cancel() // cancel the main test context to stop all servers
	require.NoError(t, g.Wait(), "all goroutines should complete without error")
	require.Len(t, srcRouterAcks, 3)
	t.Logf("testscenario: all migrations completed in %s (extraStressWorkspaces=%d)", end.Sub(start), extraStressWorkspaces)
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

package batchrouter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	asynccommon "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func testAsyncDestination(revisionID string) backendconfig.DestinationT {
	return backendconfig.DestinationT{
		ID:         "destinationID",
		Name:       "Eloqua",
		RevisionID: revisionID,
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name: "ELOQUA",
		},
	}
}

func TestInitAsyncDestinationStructInstallsInvalidManagerWithFailedAt(t *testing.T) {
	now := time.Date(2024, 2, 3, 4, 5, 6, 0, time.UTC)
	destination := testAsyncDestination("rev-1")
	batchRouter := defaultHandle("ELOQUA")
	batchRouter.now = func() time.Time { return now }
	batchRouter.asyncManagerFactory = func(
		*config.Config,
		logger.Logger,
		stats.Stats,
		*backendconfig.DestinationT,
		backendconfig.BackendConfig,
	) (asynccommon.AsyncDestinationManager, error) {
		return nil, errors.New("transient auth error")
	}

	batchRouter.initAsyncDestinationStruct(&destination)

	invalidManager, ok := batchRouter.asyncDestinationStruct[destination.ID].Manager.(*asynccommon.InvalidManager)
	require.True(t, ok)
	require.Equal(t, now, invalidManager.FailedAt)
	require.EqualError(t, invalidManager.Error, "Eloqua initialization failed with error: transient auth error")

	output := invalidManager.Upload(context.Background(), &asynccommon.AsyncDestinationStruct{
		ImportingJobIDs: []int64{1},
		FailedJobIDs:    []int64{2},
		Destination:     &destination,
	})
	require.Equal(t, []int64{1, 2}, output.AbortJobIDs)
	require.Equal(t, 2, output.AbortCount)
	require.Contains(t, output.AbortReason, "Eloqua could not be initialized")
}

func TestRefreshDestinationInvalidManagerRetryPolicy(t *testing.T) {
	interval := 5 * time.Minute
	now := time.Date(2024, 2, 3, 4, 5, 6, 0, time.UTC)

	t.Run("same revision within cooldown does not retry", func(t *testing.T) {
		destination := testAsyncDestination("rev-1")
		batchRouter := defaultHandle("ELOQUA")
		batchRouter.now = func() time.Time { return now }
		batchRouter.invalidManagerRetryInterval = config.SingleValueLoader(interval)

		invalidManager := &asynccommon.InvalidManager{
			Error:    errors.New("initial failure"),
			FailedAt: now.Add(-interval + time.Nanosecond),
		}
		batchRouter.asyncDestinationStruct[destination.ID] = &asynccommon.AsyncDestinationStruct{
			Destination: &destination,
			Manager:     invalidManager,
		}
		batchRouter.asyncManagerFactory = func(
			*config.Config,
			logger.Logger,
			stats.Stats,
			*backendconfig.DestinationT,
			backendconfig.BackendConfig,
		) (asynccommon.AsyncDestinationManager, error) {
			t.Fatal("manager factory should not be called during cooldown")
			return nil, errors.New("unexpected manager factory call")
		}

		batchRouter.refreshDestination(destination)

		require.Same(t, invalidManager, batchRouter.asyncDestinationStruct[destination.ID].Manager)
	})

	t.Run("same revision after cooldown retries and replaces invalid manager", func(t *testing.T) {
		destination := testAsyncDestination("rev-1")
		batchRouter := defaultHandle("ELOQUA")
		batchRouter.now = func() time.Time { return now }
		batchRouter.invalidManagerRetryInterval = config.SingleValueLoader(interval)
		batchRouter.asyncDestinationStruct[destination.ID] = &asynccommon.AsyncDestinationStruct{
			Destination: &destination,
			Manager: &asynccommon.InvalidManager{
				Error:    errors.New("initial failure"),
				FailedAt: now.Add(-interval),
			},
		}
		factoryCalls := 0
		manager := &mockAsyncDestinationManager{}
		batchRouter.asyncManagerFactory = func(
			*config.Config,
			logger.Logger,
			stats.Stats,
			*backendconfig.DestinationT,
			backendconfig.BackendConfig,
		) (asynccommon.AsyncDestinationManager, error) {
			factoryCalls++
			return manager, nil
		}

		batchRouter.refreshDestination(destination)

		require.Equal(t, 1, factoryCalls)
		require.Same(t, manager, batchRouter.asyncDestinationStruct[destination.ID].Manager)

		err := batchRouter.sendJobsToStorage(BatchedJobs{
			Jobs: []*jobsdb.JobT{
				{
					JobID:        99,
					EventPayload: []byte(`{"key":"value"}`),
				},
			},
			Connection: &Connection{
				Source:      backendconfig.SourceT{ID: "sourceID"},
				Destination: destination,
			},
		})
		require.NoError(t, err)
		require.Equal(t, []int64{99}, batchRouter.asyncDestinationStruct[destination.ID].ImportingJobIDs)
	})

	t.Run("same revision retry failure refreshes FailedAt", func(t *testing.T) {
		destination := testAsyncDestination("rev-1")
		batchRouter := defaultHandle("ELOQUA")
		batchRouter.now = func() time.Time { return now }
		batchRouter.invalidManagerRetryInterval = config.SingleValueLoader(interval)
		oldFailedAt := now.Add(-2 * interval)
		batchRouter.asyncDestinationStruct[destination.ID] = &asynccommon.AsyncDestinationStruct{
			Destination: &destination,
			Manager: &asynccommon.InvalidManager{
				Error:    errors.New("initial failure"),
				FailedAt: oldFailedAt,
			},
		}
		factoryCalls := 0
		batchRouter.asyncManagerFactory = func(
			*config.Config,
			logger.Logger,
			stats.Stats,
			*backendconfig.DestinationT,
			backendconfig.BackendConfig,
		) (asynccommon.AsyncDestinationManager, error) {
			factoryCalls++
			return nil, errors.New("still failing")
		}

		batchRouter.refreshDestination(destination)

		require.Equal(t, 1, factoryCalls)
		invalidManager, ok := batchRouter.asyncDestinationStruct[destination.ID].Manager.(*asynccommon.InvalidManager)
		require.True(t, ok)
		require.Equal(t, now, invalidManager.FailedAt)
		require.NotEqual(t, oldFailedAt, invalidManager.FailedAt)
		require.Contains(t, invalidManager.Error.Error(), "still failing")
	})

	t.Run("new revision retries immediately", func(t *testing.T) {
		existingDestination := testAsyncDestination("rev-1")
		updatedDestination := testAsyncDestination("rev-2")
		batchRouter := defaultHandle("ELOQUA")
		batchRouter.now = func() time.Time { return now }
		batchRouter.invalidManagerRetryInterval = config.SingleValueLoader(interval)
		batchRouter.asyncDestinationStruct[existingDestination.ID] = &asynccommon.AsyncDestinationStruct{
			Destination: &existingDestination,
			Manager: &asynccommon.InvalidManager{
				Error:    errors.New("initial failure"),
				FailedAt: now,
			},
		}
		factoryCalls := 0
		manager := &mockAsyncDestinationManager{}
		batchRouter.asyncManagerFactory = func(
			*config.Config,
			logger.Logger,
			stats.Stats,
			*backendconfig.DestinationT,
			backendconfig.BackendConfig,
		) (asynccommon.AsyncDestinationManager, error) {
			factoryCalls++
			return manager, nil
		}

		batchRouter.refreshDestination(updatedDestination)

		require.Equal(t, 1, factoryCalls)
		require.Same(t, manager, batchRouter.asyncDestinationStruct[updatedDestination.ID].Manager)
		require.Equal(t, "rev-2", batchRouter.asyncDestinationStruct[updatedDestination.ID].Destination.RevisionID)
	})

	t.Run("healthy manager same revision does not retry", func(t *testing.T) {
		destination := testAsyncDestination("rev-1")
		batchRouter := defaultHandle("ELOQUA")
		manager := &mockAsyncDestinationManager{}
		batchRouter.asyncDestinationStruct[destination.ID] = &asynccommon.AsyncDestinationStruct{
			Destination: &destination,
			Manager:     manager,
		}
		batchRouter.asyncManagerFactory = func(
			*config.Config,
			logger.Logger,
			stats.Stats,
			*backendconfig.DestinationT,
			backendconfig.BackendConfig,
		) (asynccommon.AsyncDestinationManager, error) {
			t.Fatal("manager factory should not be called for a healthy unchanged destination")
			return nil, errors.New("unexpected manager factory call")
		}

		batchRouter.refreshDestination(destination)

		require.Same(t, manager, batchRouter.asyncDestinationStruct[destination.ID].Manager)
	})

	t.Run("zero FailedAt retries immediately", func(t *testing.T) {
		destination := testAsyncDestination("rev-1")
		batchRouter := defaultHandle("ELOQUA")
		batchRouter.invalidManagerRetryInterval = config.SingleValueLoader(interval)
		batchRouter.asyncDestinationStruct[destination.ID] = &asynccommon.AsyncDestinationStruct{
			Destination: &destination,
			Manager: &asynccommon.InvalidManager{
				Error: errors.New("legacy failure"),
			},
		}
		factoryCalls := 0
		manager := &mockAsyncDestinationManager{}
		batchRouter.asyncManagerFactory = func(
			*config.Config,
			logger.Logger,
			stats.Stats,
			*backendconfig.DestinationT,
			backendconfig.BackendConfig,
		) (asynccommon.AsyncDestinationManager, error) {
			factoryCalls++
			return manager, nil
		}

		batchRouter.refreshDestination(destination)

		require.Equal(t, 1, factoryCalls)
		require.Same(t, manager, batchRouter.asyncDestinationStruct[destination.ID].Manager)
	})
}

func TestInvalidManagerRetryLoopDoesNotBlockAsyncUploadWorker(t *testing.T) {
	misc.Init()
	interval := 20 * time.Millisecond
	invalidDestination := testAsyncDestination("rev-1")
	healthyDestination := testAsyncDestination("rev-1")
	healthyDestination.ID = "healthyDestinationID"

	batchRouter := defaultHandle("ELOQUA")
	batchRouter.now = time.Now
	batchRouter.invalidManagerRetryInterval = config.SingleValueLoader(interval)
	batchRouter.asyncUploadWorkerTimeout = config.SingleValueLoader(time.Millisecond)
	batchRouter.destinationsMap[invalidDestination.ID] = &routerutils.DestinationWithSources{
		Destination: invalidDestination,
	}
	batchRouter.destinationsMap[healthyDestination.ID] = &routerutils.DestinationWithSources{
		Destination: healthyDestination,
		Sources:     []backendconfig.SourceT{{ID: "sourceID", WorkspaceID: "workspaceID"}},
	}
	batchRouter.uploadIntervalMap[healthyDestination.ID] = 0
	batchRouter.asyncDestinationStruct[invalidDestination.ID] = &asynccommon.AsyncDestinationStruct{
		Destination: &invalidDestination,
		Manager: &asynccommon.InvalidManager{
			Error:    errors.New("initial failure"),
			FailedAt: time.Now().Add(-interval),
		},
	}
	uploadCalled := make(chan struct{}, 1)
	batchRouter.asyncDestinationStruct[healthyDestination.ID] = &asynccommon.AsyncDestinationStruct{
		Destination: &healthyDestination,
		Exists:      true,
		CanUpload:   true,
		CreatedAt:   time.Now().Add(-time.Second),
		FileName:    "testdata/uploadData.txt",
		Manager: &mockAsyncDestinationManager{
			uploadFunc: func(context.Context, *asynccommon.AsyncDestinationStruct) asynccommon.AsyncUploadOutput {
				uploadCalled <- struct{}{}
				return asynccommon.AsyncUploadOutput{DestinationID: healthyDestination.ID}
			},
		},
	}

	retryStarted := make(chan struct{}, 1)
	unblockRetry := make(chan struct{})
	batchRouter.asyncManagerFactory = func(
		_ *config.Config,
		_ logger.Logger,
		_ stats.Stats,
		destination *backendconfig.DestinationT,
		_ backendconfig.BackendConfig,
	) (asynccommon.AsyncDestinationManager, error) {
		if destination.ID == invalidDestination.ID {
			retryStarted <- struct{}{}
			<-unblockRetry
		}
		return &mockAsyncDestinationManager{}, nil
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	defer close(unblockRetry)
	go batchRouter.retryInvalidAsyncDestinationManagersOnce(ctx)
	require.Eventually(t, func() bool {
		select {
		case <-retryStarted:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)

	go batchRouter.asyncUploadWorker(ctx)

	require.Eventually(t, func() bool {
		select {
		case <-uploadCalled:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func TestRetryInvalidAsyncDestinationManagersOnceRetriesStaleInvalidManagers(t *testing.T) {
	interval := 5 * time.Minute
	now := time.Date(2024, 2, 3, 4, 5, 6, 0, time.UTC)
	destination := testAsyncDestination("rev-1")
	batchRouter := defaultHandle("ELOQUA")
	batchRouter.now = func() time.Time { return now }
	batchRouter.invalidManagerRetryInterval = config.SingleValueLoader(interval)
	batchRouter.destinationsMap[destination.ID] = &routerutils.DestinationWithSources{
		Destination: destination,
	}
	batchRouter.asyncDestinationStruct[destination.ID] = &asynccommon.AsyncDestinationStruct{
		Destination: &destination,
		Manager: &asynccommon.InvalidManager{
			Error:    errors.New("initial failure"),
			FailedAt: now.Add(-interval),
		},
	}
	factoryCalls := 0
	manager := &mockAsyncDestinationManager{}
	batchRouter.asyncManagerFactory = func(
		*config.Config,
		logger.Logger,
		stats.Stats,
		*backendconfig.DestinationT,
		backendconfig.BackendConfig,
	) (asynccommon.AsyncDestinationManager, error) {
		factoryCalls++
		return manager, nil
	}

	batchRouter.retryInvalidAsyncDestinationManagersOnce(context.Background())

	require.Equal(t, 1, factoryCalls)
	require.Same(t, manager, batchRouter.asyncDestinationStruct[destination.ID].Manager)
}

func TestRetryInvalidAsyncDestinationManagersOnceUsesCurrentDestinationRevision(t *testing.T) {
	interval := 5 * time.Minute
	now := time.Date(2024, 2, 3, 4, 5, 6, 0, time.UTC)
	staleDestination := testAsyncDestination("rev-1")
	currentDestination := testAsyncDestination("rev-2")
	batchRouter := defaultHandle("ELOQUA")
	batchRouter.now = func() time.Time { return now }
	batchRouter.invalidManagerRetryInterval = config.SingleValueLoader(interval)
	batchRouter.destinationsMap[currentDestination.ID] = &routerutils.DestinationWithSources{
		Destination: currentDestination,
	}
	batchRouter.asyncDestinationStruct[staleDestination.ID] = &asynccommon.AsyncDestinationStruct{
		Destination: &staleDestination,
		Manager: &asynccommon.InvalidManager{
			Error:    errors.New("initial failure"),
			FailedAt: now.Add(-interval),
		},
	}
	manager := &mockAsyncDestinationManager{}
	batchRouter.asyncManagerFactory = func(
		_ *config.Config,
		_ logger.Logger,
		_ stats.Stats,
		destination *backendconfig.DestinationT,
		_ backendconfig.BackendConfig,
	) (asynccommon.AsyncDestinationManager, error) {
		require.Equal(t, "rev-2", destination.RevisionID)
		return manager, nil
	}

	batchRouter.retryInvalidAsyncDestinationManagersOnce(context.Background())

	require.Equal(t, "rev-2", batchRouter.asyncDestinationStruct[currentDestination.ID].Destination.RevisionID)
	require.Same(t, manager, batchRouter.asyncDestinationStruct[currentDestination.ID].Manager)

	batchRouter.refreshDestination(staleDestination)

	require.Equal(t, "rev-2", batchRouter.asyncDestinationStruct[currentDestination.ID].Destination.RevisionID)
	require.Same(t, manager, batchRouter.asyncDestinationStruct[currentDestination.ID].Manager)
}

package jobsdb

import (
	"context"

	"github.com/rudderlabs/rudder-go-kit/jsonparser"
)

type PendingEventsRegistry interface {
	IncreasePendingEvents(tablePrefix, workspaceID, destType, destinationID string, value float64)
	DecreasePendingEvents(tablePrefix, workspaceID, destType, destinationID string, value float64)
}

// PendingEventsOpt configures a pendingEventsJobsDB.
type PendingEventsOpt func(*pendingEventsJobsDB)

// WithConsumerAsDestinationID makes pending-events accounting derive the destination ID from a
// job's consumers (resp. a status's consumer) instead of `parameters->>'destination_id'`. Intended
// for multi-consumer handles where a consumer IS a destination ID: a single job pending for N
// consumers contributes one pending event per consumer, and a per-consumer terminal status decrements
// exactly that destination.
func WithConsumerAsDestinationID() PendingEventsOpt {
	return func(pejdb *pendingEventsJobsDB) {
		pejdb.storeDestinationIDs = consumerStoreDestinationIDs
		pejdb.statusDestinationID = consumerStatusDestinationID
	}
}

// NewPendingEventsJobsDB wraps a JobsDB with pending events metrics collection. By default the
// destination ID is read from `parameters->>'destination_id'`; pass WithConsumerAsDestinationID to
// treat consumers as destination IDs (multi-consumer handles).
func NewPendingEventsJobsDB(jobsDB JobsDB, registry PendingEventsRegistry, opts ...PendingEventsOpt) JobsDB {
	pejdb := &pendingEventsJobsDB{
		JobsDB:              jobsDB,
		registry:            registry,
		storeDestinationIDs: parametersStoreDestinationIDs,
		statusDestinationID: parametersStatusDestinationID,
	}
	for _, opt := range opts {
		opt(pejdb)
	}
	return pejdb
}

type pendingEventsJobsDB struct {
	JobsDB
	registry PendingEventsRegistry
	// storeDestinationIDs returns the destination ID(s) a stored job is pending for.
	storeDestinationIDs func(job *JobT) []string
	// statusDestinationID returns the destination ID a status row belongs to.
	statusDestinationID func(status *JobStatusT) string
}

// parametersStoreDestinationIDs reads the single destination ID from the job's parameters.
func parametersStoreDestinationIDs(job *JobT) []string {
	return []string{jsonparser.GetStringOrEmpty(job.Parameters, "destination_id")}
}

// parametersStatusDestinationID reads the destination ID from the status's job parameters.
func parametersStatusDestinationID(status *JobStatusT) string {
	if status.JobParameters == nil {
		return ""
	}
	return jsonparser.GetStringOrEmpty(status.JobParameters, "destination_id")
}

// consumerStoreDestinationIDs treats the job's consumers as its destination IDs.
func consumerStoreDestinationIDs(job *JobT) []string {
	if len(job.Consumers) == 0 {
		return []string{""}
	}
	return job.Consumers
}

// consumerStatusDestinationID treats the status's consumer as its destination ID.
func consumerStatusDestinationID(status *JobStatusT) string {
	return status.Consumer
}

func (pejdb *pendingEventsJobsDB) Store(ctx context.Context, jobList []*JobT) error {
	return pejdb.WithStoreSafeTx(ctx, func(tx StoreSafeTx) error {
		return pejdb.StoreInTx(ctx, tx, jobList)
	})
}

func (pejdb *pendingEventsJobsDB) StoreInTx(ctx context.Context, tx StoreSafeTx, jobList []*JobT) error {
	// Register the pending-events increase only after the underlying store succeeds. The
	// store may be retried internally on a stale dataset list; when it runs inside a
	// caller-provided transaction (WithStoreSafeTxFromTx) that transaction is reused across
	// retries, so registering before the store would double-count on every retry.
	if err := pejdb.JobsDB.StoreInTx(ctx, tx, jobList); err != nil {
		return err
	}
	tx.Tx().AddSuccessListener(func() {
		counters := make(map[string]map[string]map[string]float64) // workspaceID -> destType -> destinationID -> count
		for _, job := range jobList {
			workspaceID := job.WorkspaceId
			destType := job.CustomVal
			for _, destinationID := range pejdb.storeDestinationIDs(job) {
				if _, ok := counters[workspaceID]; !ok {
					counters[workspaceID] = make(map[string]map[string]float64)
				}
				if _, ok := counters[workspaceID][destType]; !ok {
					counters[workspaceID][destType] = make(map[string]float64)
				}
				counters[workspaceID][destType][destinationID]++
			}
		}
		for workspaceID, destTypeMap := range counters {
			for destType, destinationIDMap := range destTypeMap {
				for destinationID, count := range destinationIDMap {
					pejdb.registry.IncreasePendingEvents(pejdb.Identifier(), workspaceID, destType, destinationID, count)
				}
			}
		}
	})
	return nil
}

func (pejdb *pendingEventsJobsDB) UpdateJobStatus(ctx context.Context, statusList []*JobStatusT) error {
	return pejdb.WithUpdateSafeTx(ctx, func(tx UpdateSafeTx) error {
		return pejdb.UpdateJobStatusInTx(ctx, tx, statusList)
	})
}

func (pejdb *pendingEventsJobsDB) UpdateJobStatusInTx(ctx context.Context, tx UpdateSafeTx, statusList []*JobStatusT) error {
	// Register the pending-events decrease only after the underlying update succeeds: the
	// update retries internally on a stale dataset list against the (possibly reused)
	// transaction, so registering before it would double-count on every retry.
	if err := pejdb.JobsDB.UpdateJobStatusInTx(ctx, tx, statusList); err != nil {
		return err
	}
	tx.Tx().AddSuccessListener(func() {
		counters := make(map[string]map[string]map[string]float64) // workspaceID -> destType -> destinationID -> count
		for _, status := range statusList {
			if _, ok := terminalStates[status.JobState]; !ok {
				continue
			}
			workspaceID := status.WorkspaceId
			destType := status.CustomVal
			destinationID := pejdb.statusDestinationID(status)
			if _, ok := counters[workspaceID]; !ok {
				counters[workspaceID] = make(map[string]map[string]float64)
			}
			if _, ok := counters[workspaceID][destType]; !ok {
				counters[workspaceID][destType] = make(map[string]float64)
			}
			counters[workspaceID][destType][destinationID]++
		}
		for workspaceID, destTypeMap := range counters {
			for destType, destinationIDMap := range destTypeMap {
				for destinationID, count := range destinationIDMap {
					pejdb.registry.DecreasePendingEvents(pejdb.Identifier(), workspaceID, destType, destinationID, count)
				}
			}
		}
	})
	return nil
}

package jobsdb

import (
	"context"

	"github.com/tidwall/gjson"
)

type PendingEventsRegistry interface {
	IncreasePendingEvents(tablePrefix, workspaceID, destType, destinationID string, value float64)
	DecreasePendingEvents(tablePrefix, workspaceID, destType, destinationID string, value float64)
}

// NewPendingEventsJobsDB wraps a JobsDB with pending events metrics collection
func NewPendingEventsJobsDB(jobsDB JobsDB, registry PendingEventsRegistry) JobsDB {
	return &pendingEventsJobsDB{
		JobsDB:   jobsDB,
		registry: registry,
	}
}

type pendingEventsJobsDB struct {
	JobsDB
	registry PendingEventsRegistry
}

func (pejdb *pendingEventsJobsDB) Store(ctx context.Context, jobList []*JobT) error {
	return pejdb.WithStoreSafeTx(ctx, func(tx StoreSafeTx) error {
		return pejdb.StoreInTx(ctx, tx, jobList)
	})
}

func (pejdb *pendingEventsJobsDB) StoreInTx(ctx context.Context, tx StoreSafeTx, jobList []*JobT) error {
	tx.Tx().AddSuccessListener(func() {
		counters := make(map[string]map[string]map[string]float64) // workspaceID -> destType -> destinationID -> count
		for _, job := range jobList {
			workspaceID := job.WorkspaceId
			destType := job.CustomVal
			destinationID := gjson.GetBytes(job.Parameters, "destination_id").String()
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
					pejdb.registry.IncreasePendingEvents(pejdb.Identifier(), workspaceID, destType, destinationID, count)
				}
			}
		}
	})
	return pejdb.JobsDB.StoreInTx(ctx, tx, jobList)
}

func (pejdb *pendingEventsJobsDB) UpdateJobStatus(ctx context.Context, statusList []*JobStatusT) error {
	return pejdb.WithUpdateSafeTx(ctx, func(tx UpdateSafeTx) error {
		return pejdb.UpdateJobStatusInTx(ctx, tx, statusList)
	})
}

func (pejdb *pendingEventsJobsDB) UpdateJobStatusInTx(ctx context.Context, tx UpdateSafeTx, statusList []*JobStatusT) error {
	tx.Tx().AddSuccessListener(func() {
		counters := make(map[string]map[string]map[string]float64) // workspaceID -> destType -> destinationID -> count
		for _, status := range statusList {
			if _, ok := terminalStates[status.JobState]; !ok {
				continue
			}
			workspaceID := status.WorkspaceId
			destType := status.CustomVal
			var destinationID string
			if status.JobParameters != nil {
				destinationID = gjson.GetBytes(status.JobParameters, "destination_id").String()
			}
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
	return pejdb.JobsDB.UpdateJobStatusInTx(ctx, tx, statusList)
}

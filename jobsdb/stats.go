package jobsdb

import (
	"sync"

	uuid "github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/services/metric"
)

var statesIndex map[string]jobStateT
var once sync.Once

type statsHandle struct {
	JobsDB
}

func (r *statsHandle) Store(jobList []*JobT) error {
	err := r.JobsDB.Store(jobList)
	if err != nil {
		return err
	}
	r.add(jobList)
	return nil
}

func (r *statsHandle) StoreInTx(tx StoreSafeTx, jobList []*JobT) error {
	err := r.JobsDB.StoreInTx(tx, jobList)
	if err != nil {
		return err
	}
	r.add(jobList)
	return nil
}

func (r *statsHandle) StoreWithRetryEach(jobList []*JobT) map[uuid.UUID]string {
	res := r.JobsDB.StoreWithRetryEach(jobList)

	var successful []*JobT
	for i := range jobList {
		if _, ok := res[jobList[i].UUID]; !ok {
			successful = append(successful, jobList[i])
		}
	}
	r.add(successful)

	return res
}

func (r *statsHandle) StoreWithRetryEachInTx(tx StoreSafeTx, jobList []*JobT) map[uuid.UUID]string {
	res := r.JobsDB.StoreWithRetryEachInTx(tx, jobList)

	var successful []*JobT
	for i := range jobList {
		if _, ok := res[jobList[i].UUID]; !ok {
			successful = append(successful, jobList[i])
		}
	}
	if len(successful) > 0 {
		tx.AddSuccessListener(func() {
			r.add(successful)
		})
	}

	return res

}
func (r *statsHandle) UpdateJobStatus(statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) error {
	err := r.JobsDB.UpdateJobStatus(statusList, customValFilters, parameterFilters)
	if err != nil {
		return err
	}
	r.dec(statusList)
	return nil
}

func (r *statsHandle) UpdateJobStatusInTx(tx UpdateSafeTx, statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) error {
	err := r.JobsDB.UpdateJobStatusInTx(tx, statusList, customValFilters, parameterFilters)
	if err != nil {
		return err
	}
	tx.AddSuccessListener(func() {
		r.dec(statusList)
	})
	return nil
}

func (r *statsHandle) add(jobList []*JobT) {
	metric.PendingEvents(r.Identifier(), "GLOBAL", "GLOBAL").Add(float64(len(jobList)))
}

func (r *statsHandle) dec(statusList []*JobStatusT) {
	once.Do(func() {
		statesIndex = map[string]jobStateT{}
		for _, jobState := range jobStates {
			statesIndex[jobState.State] = jobState
		}
	})
	var count int
	for i := range statusList {
		if statesIndex[statusList[i].JobState].isTerminal {
			count++
		}
	}
	metric.PendingEvents(r.Identifier(), "GLOBAL", "GLOBAL").Sub(float64(count))
}

func WithStats(handle *HandleT) JobsDB {
	return &statsHandle{handle}
}

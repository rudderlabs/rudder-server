package jobsdb

import (
	"database/sql"
	"time"

	"github.com/rudderlabs/rudder-server/distributed"
	uuid "github.com/satori/go.uuid"
)

type CustomerQueue struct {
	GatewayJobsdb      JobsDB
	RouterJobsdb       JobsDB
	BatchrouterJobsddb JobsDB
	ProcerrorJobsdb    JobsDB
}

var customerQueues map[string]*CustomerQueue

func SetupCustomerQueues() {
	customerQueues = make(map[string]*CustomerQueue)
	customers := distributed.GetCustomerList()

	for _, customer := range customers {
		var gatewayDB HandleT
		var routerDB HandleT
		var batchRouterDB HandleT
		var procErrorDB HandleT

		//TODO: fix values passed
		gatewayDB.Setup(ReadWrite, false, customer+"_"+"gw", time.Hour*10000, "", true, QueryFiltersT{})
		//setting up router, batch router, proc error DBs also irrespective of server mode
		routerDB.Setup(ReadWrite, false, customer+"_"+"rt", time.Hour*10000, "", true, QueryFiltersT{})
		batchRouterDB.Setup(ReadWrite, false, customer+"_"+"batch_rt", time.Hour*10000, "", true, QueryFiltersT{})
		procErrorDB.Setup(ReadWrite, false, customer+"_"+"proc_error", time.Hour*10000, "", false, QueryFiltersT{})

		customerQueues[customer] = &CustomerQueue{
			GatewayJobsdb:      &gatewayDB,
			RouterJobsdb:       &routerDB,
			BatchrouterJobsddb: &batchRouterDB,
			ProcerrorJobsdb:    &procErrorDB,
		}
	}
}

func GetCustomerList(jobList []*JobT) map[string]int {
	customerListMap := make(map[string]int)
	for _, job := range jobList {
		if _, ok := customerListMap[job.Customer]; !ok {
			customerListMap[job.Customer] = 0
		}
	}
	return customerListMap
}

func getQueueForCustomer(customer, queue string) JobsDB {
	customerQueue := customerQueues[customer]
	switch queue {
	case "gw":
		return customerQueue.GatewayJobsdb
	case "rt":
		return customerQueue.RouterJobsdb
	case "batch_rt":
		return customerQueue.BatchrouterJobsddb
	case "proc_error":
		return customerQueue.ProcerrorJobsdb
	}

	panic("Unknow queue")
}

func Store(jobList []*JobT, queue string) error {
	//TODO remove loop on jobList again for performance benefits
	//TODO handle errors properly
	customerJobListMap := make(map[string][]*JobT)
	for _, job := range jobList {
		if _, ok := customerJobListMap[job.Customer]; !ok {
			customerJobListMap[job.Customer] = make([]*JobT, 0)
		}
		customerJobListMap[job.Customer] = append(customerJobListMap[job.Customer], job)
	}

	for customer, list := range customerJobListMap {
		StoreJobsForCustomer(customer, queue, list)
	}
	return nil
}

func StoreJobsForCustomer(customer string, queue string, list []*JobT) error {
	getQueueForCustomer(customer, queue).Store(list)
	return nil
}

func StoreWithRetryEach(jobList []*JobT, queue string) map[uuid.UUID]string {
	//TODO remove loop on jobList again for performance benefits
	//TODO handle errors properly
	customerJobListMap := make(map[string][]*JobT)
	for _, job := range jobList {
		if _, ok := customerJobListMap[job.Customer]; !ok {
			customerJobListMap[job.Customer] = make([]*JobT, 0)
		}
		customerJobListMap[job.Customer] = append(customerJobListMap[job.Customer], job)
	}

	maps := make([]map[uuid.UUID]string, 0)
	for customer, list := range customerJobListMap {
		maps = append(maps, getQueueForCustomer(customer, queue).StoreWithRetryEach(list))
	}
	return MergeMaps(maps...)
}

func DeleteExecuting(params GetQueryParamsT, queueType string) {
	for customer := range customerQueues {
		getQueueForCustomer(customer, queueType).DeleteExecuting(params)
	}
}

func BeginGlobalTransaction(customer string, queueType string) *sql.Tx {
	return getQueueForCustomer(customer, queueType).BeginGlobalTransaction()
}

func AcquireUpdateJobStatusLocks(customer string, queueType string) {
	getQueueForCustomer(customer, queueType).AcquireUpdateJobStatusLocks()
}

func UpdateJobStatusInTxn(txn *sql.Tx, statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT, customer string, queueType string) error {
	err := getQueueForCustomer(customer, queueType).UpdateJobStatusInTxn(txn, statusList, customValFilters, parameterFilters)
	return err

}

func CommitTransaction(txn *sql.Tx, customer string, queueType string) {
	getQueueForCustomer(customer, queueType).CommitTransaction(txn)
}

func ReleaseUpdateJobStatusLocks(customer string, queueType string) {
	getQueueForCustomer(customer, queueType).ReleaseUpdateJobStatusLocks()
}

func GetToRetry(params GetQueryParamsT, customer string, queueType string) []*JobT {
	return getQueueForCustomer(customer, queueType).GetToRetry(params)
}

func GetUnprocessed(params GetQueryParamsT, customer string, queueType string) []*JobT {
	return getQueueForCustomer(customer, queueType).GetUnprocessed(params)
}

func MergeMaps(maps ...map[uuid.UUID]string) (result map[uuid.UUID]string) {
	result = make(map[uuid.UUID]string)
	for _, m := range maps {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}

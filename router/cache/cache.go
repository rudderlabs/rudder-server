//HOW TO USE:
//Public Functions: Init, GetCount, Store, UpdateStatus, GetJobs
//Be careful before making private functions of the package to public. Locks can cause an issue.

package cache

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

const CACHE_COUNT_LIMIT = int(1 * 1e6) //1M
const CACHE_PAGE_LIMIT = int(4 * 1e9)  //4GB
const INSERT_JOB_LIMIT = int(1 * 1e3)

const Success = "succeeded"

type JobsCache struct {
	currentCount    int
	customers       map[string]int
	sqlDB           *sql.DB
	cacheCountLimit int
	cachePageLimit  int
}

type JobId struct {
	Id       int
	Customer string
}

type Job struct {
	JobId        int
	UUID         string
	UserId       string
	Parameters   json.RawMessage
	CustomVal    string
	Customer     string
	EventPayload json.RawMessage
	JobState     string
}

const cacheTableName = "rt_jobs"

var lock sync.Mutex

var jobColumns = []string{"job_id", "uuid", "user_id", "parameters", "custom_val", "customer", "event_payload", "job_state"}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func (c *JobsCache) Init() error {

	lock.Lock()
	defer lock.Unlock()

	var err error

	c.sqlDB, err = sql.Open("sqlite3", ":memory:?_auto_vacuum=1&cache=shared")
	c.cacheCountLimit = CACHE_COUNT_LIMIT //TODO: use env to set this and give CACHE_COUNT_LIMIT as default
	c.cachePageLimit = CACHE_PAGE_LIMIT   //TODO: use env to set this and give CACHE_PAGE_LIMIT as default

	err = c.createTable(cacheTableName)
	if err != nil {
		return err
	}
	c.customers = make(map[string]int)

	return c.refresh()
}

func (c *JobsCache) createTable(tableName string) error {

	createTable := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		"job_id" INT PRIMARY KEY,		
		"uuid" TEXT,
		"user_id" TEXT,
		"parameters" TEXT,
		"custom_val" TEXT,
		"customer" TEXT,
		"event_payload"	TEXT,
		"job_state" TEXT
	  );`, tableName)

	createTableQuery, err := c.sqlDB.Prepare(createTable)
	defer createTableQuery.Close()
	if err != nil {
		return err
	}
	_, err = createTableQuery.Exec()
	if err != nil {
		return err
	}

	indexOnTable := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%[1]s
	ON %[1]s (custom_val, customer, job_state);`, tableName)

	indexOnTableQuery, err := c.sqlDB.Prepare(indexOnTable)
	defer indexOnTableQuery.Close()
	if err != nil {
		log.Fatal(err.Error())
	}

	_, err = indexOnTableQuery.Exec()
	return err
}

func (c *JobsCache) GetCount() (int, error) {

	lock.Lock()
	defer lock.Unlock()

	countQuery := fmt.Sprintf("select count(*) from %s;", cacheTableName)
	row, err := c.sqlDB.Query(countQuery)
	defer row.Close()
	if err != nil {
		return 0, err
	}

	count := -1
	for row.Next() {
		err = row.Scan(&count)
		if err != nil {
			return 0, err
		}
	}
	return count, nil
}

func (c *JobsCache) getPageCount() (int, error) {
	checkIndex := fmt.Sprintf("PRAGMA page_count;")
	rows, err := c.sqlDB.Query(checkIndex)
	defer rows.Close()
	if err != nil {
		return 0, err
	}

	var count int
	for rows.Next() {
		if err := rows.Scan(&count); err != nil {
			log.Fatal(err)
		}
	}
	return count, nil
}

func (c *JobsCache) getPageSize() (int, error) {
	checkIndex := fmt.Sprintf("PRAGMA page_size;")
	rows, err := c.sqlDB.Query(checkIndex)
	defer rows.Close()
	if err != nil {
		return 0, err
	}

	var count int
	for rows.Next() {
		if err := rows.Scan(&count); err != nil {
			log.Fatal(err)
		}
	}
	return count, nil
}

func (c *JobsCache) getCustomerCount(customer string) (int, error) {

	countQuery := fmt.Sprintf("select count(*) from %s where customer = '%s';", cacheTableName, customer)
	row, err := c.sqlDB.Query(countQuery)
	defer row.Close()
	if err != nil {
		return 0, err
	}

	count := -1
	for row.Next() {
		err = row.Scan(&count)
		if err != nil {
			return 0, err
		}
	}
	return count, nil
}

func (c *JobsCache) insertIntotable(valuesSlice []string, values []interface{}) error {

	storeJob := fmt.Sprintf(`INSERT INTO %s(%s) VALUES %s`, cacheTableName, strings.Join(jobColumns, ","), strings.Join(valuesSlice, ","))
	storeJobQuery, err := c.sqlDB.Prepare(storeJob)
	defer storeJobQuery.Close()
	if err != nil {
		return err
	}

	_, err = storeJobQuery.Exec(values...)
	return err
}

func (c *JobsCache) Store(jobs []Job, forceCustomers []string) error {

	lock.Lock()
	defer lock.Unlock()

	if len(jobs) == 0 {
		fmt.Println("No jobs are given")
		return nil
	}

	pageSize, err := c.getPageSize()
	if err != nil {
		return err
	}
	pageCount, err := c.getPageCount()
	if err != nil {
		return err
	}

	if c.currentCount > c.cacheCountLimit || pageSize*pageCount > c.cachePageLimit {
		err := c.evict(pageSize, pageCount)
		if err != nil {
			return err
		}
	}

	placeHolder := []string{}
	values := []interface{}{}

	for i := 0; i < len(jobColumns); i++ {
		placeHolder = append(placeHolder, "?")
	}

	placeHolderString := strings.Join(placeHolder, ",")
	if len(placeHolder) > 0 {
		placeHolderString = "(" + placeHolderString + ")"
	}

	valuesSlice := []string{}
	insertJobLimit := INSERT_JOB_LIMIT //TODO: Use env to set this and give default value to INSERT_JOB_LIMIT

	for i := 0; i < len(jobs); i++ {

		if _, ok := c.customers[jobs[i].Customer]; ok || stringInSlice(jobs[i].Customer, forceCustomers) {
			valuesSlice = append(valuesSlice, placeHolderString)
			values = append(values, jobs[i].JobId, jobs[i].UUID, jobs[i].UserId, jobs[i].Parameters, jobs[i].CustomVal, jobs[i].Customer, jobs[i].EventPayload, jobs[i].JobState)
			c.customers[jobs[i].Customer] += 1
		}

		if len(valuesSlice)%insertJobLimit == 0 || i+1 == len(jobs) {

			if len(valuesSlice) == 0 {
				fmt.Println("No new job has been inserted into the table")
				return nil
			}

			err := c.insertIntotable(valuesSlice, values)
			if err != nil {
				return err
			}

			c.currentCount += len(valuesSlice)
			fmt.Println("Jobs Inserted: ", c.currentCount)

			values = []interface{}{}
			valuesSlice = []string{}

		}
	}
	return nil
}

func (c *JobsCache) UpdateStatus(status string, jobIds []JobId) error {

	lock.Lock()
	defer lock.Unlock()

	if len(jobIds) == 0 {
		fmt.Println("No jobIds are given")
		return nil
	}

	jobIdsToString := []string{}
	for _, jobid := range jobIds {
		jobIdsToString = append(jobIdsToString, strconv.Itoa(jobid.Id))
	}

	var query string

	if status == Success {
		query = fmt.Sprintf(`DELETE FROM %s
		WHERE
			job_id in (%s)`, cacheTableName, strings.Join(jobIdsToString, ","))
		c.currentCount -= len(jobIds)
		for _, jobid := range jobIds {
			c.customers[jobid.Customer] -= 1
		}
	} else {
		query = fmt.Sprintf(`UPDATE %s
			SET job_state = '%s'
			WHERE
				job_id in (%s)`, cacheTableName, status, strings.Join(jobIdsToString, ","))
	}

	querySmt, err := c.sqlDB.Prepare(query)
	defer querySmt.Close()
	if err != nil {
		return err
	}

	_, err = querySmt.Exec()
	if err != nil {
		return err
	}
	return nil
}

func getSelectQuery(expectedStatus string, customers map[string]int, customVal string) []string {

	queries := []string{}

	for customer, count := range customers {

		if _, ok := customers[customer]; !ok {
			continue
		}

		selectQuery := fmt.Sprintf(`SELECT * FROM (SELECT 
		jobs.job_id, 
		jobs.uuid, 
		jobs.user_id, 
		jobs.parameters, 
		jobs.custom_val, 
		jobs.customer,
		jobs.event_payload,
		jobs.job_state
		FROM 
			%s AS jobs
		WHERE
			customer = '%s' AND custom_val = '%s' AND job_state = '%s'
		ORDER BY 
			jobs.job_id
		LIMIT 
			%s)`, cacheTableName, customer, customVal, expectedStatus, strconv.Itoa(count))
		queries = append(queries, selectQuery)
	}

	return queries
}

func iterateRows(row *sql.Rows, customers map[string]int) ([]Job, map[string]int, error) {

	jobs := []Job{}

	for row.Next() {
		job := Job{}
		err := row.Scan(&job.JobId, &job.UUID, &job.UserId, &job.Parameters, &job.CustomVal,
			&job.Customer, &job.EventPayload, &job.JobState)
		if err != nil {
			return nil, nil, err
		}
		jobs = append(jobs, job)
		customers[job.Customer] -= 1
	}

	keysToDelete := []string{}
	for key, value := range customers {
		if value == 0 {
			keysToDelete = append(keysToDelete, key)
		}
	}

	for _, data := range keysToDelete {
		delete(customers, data)
	}

	return jobs, customers, nil
}

func addMaps(a map[string]int, b map[string]int) map[string]int {
	for k, v := range b {
		a[k] = v
	}
	return a
}

func (c *JobsCache) GetJobs(expectedStatus string, customVal string, customers map[string]int) ([]Job, map[string]int, error) {

	lock.Lock()
	defer lock.Unlock()

	var err error
	if len(customers) == 0 {
		fmt.Println("Customer map is empty")
		return nil, nil, nil
	}

	size := 50
	//Union call is being broken into sizes of 50. Doing this results into better time and sqlite is exiting in docker/kubernetes if the union size is too high.
	times := (len(customers) + size - 1) / size
	jobs := []Job{}
	customersMap := make(map[string]int)
	customerKeys := []string{}
	for key := range customers {
		customerKeys = append(customerKeys, key)
	}
	for i := 0; i < times; i++ {
		val := make(map[string]int)
		for j := i * size; j < (i+1)*size && j < len(customerKeys); j++ {
			val[customerKeys[j]] = customers[customerKeys[j]]
		}
		queriesSlice := getSelectQuery(expectedStatus, val, customVal)
		fullQuery := strings.Join(queriesSlice, " UNION ")

		row, err := c.sqlDB.Query(fullQuery)
		defer row.Close()
		if err != nil {
			return nil, nil, err
		}

		jobsTemp, customersTemp, err := iterateRows(row, val)
		jobs = append(jobs, jobsTemp...)
		addMaps(customersMap, customersTemp)
	}
	return jobs, customersMap, err
}

func (c *JobsCache) refresh() error {
	//TODO:
	//Needs data from the rt_jobs in db
	//Should get those customers whose first entry is the most recent one
	//Update the meta-data
	return nil
}

func (c *JobsCache) getCustomerToEvict() (string, error) {

	customerEvictOrder := fmt.Sprintf(`SELECT customer FROM 
			(
				SELECT customer, MIN(job_id) as job_id
				FROM %s
			)`, cacheTableName)

	row, err := c.sqlDB.Query(customerEvictOrder)
	defer row.Close()
	if err != nil {
		return "", err
	}

	var customer string
	for row.Next() {
		err = row.Scan(&customer)
		if err != nil {
			return "", err
		}
	}
	return customer, nil
}

func (c *JobsCache) evict(pageSize int, pageCount int) error {

	for c.currentCount > c.cacheCountLimit || pageSize*pageCount > c.cachePageLimit {

		customer, err := c.getCustomerToEvict()
		delete(c.customers, customer)

		count, err := c.getCustomerCount(customer)
		if err != nil {
			return err
		}

		//DELETE FROM %[1]s WHERE job_id in (SELECT job_id FROM %[1]s WHERE customer = '%[2]s' ORDER BY job_id DESC LIMIT %[3]s)
		deleteTable := fmt.Sprintf(`DELETE FROM %[1]s WHERE customer = '%[2]s';`, cacheTableName, customer)

		deleteTableQuery, err := c.sqlDB.Prepare(deleteTable)
		defer deleteTableQuery.Close()
		if err != nil {
			return err
		}

		_, err = deleteTableQuery.Exec()
		if err != nil {
			return err
		}

		fmt.Println("All entries of this customer have been removed from cache:", customer, count)
		c.currentCount -= count

		pageSize, err = c.getPageSize()
		if err != nil {
			return err
		}
		pageCount, err = c.getPageCount()
		if err != nil {
			return err
		}
	}

	if c.currentCount <= 0 {
		return fmt.Errorf("Cache is empty")
	}
	return nil
}

package helpers

import (
	"bytes"
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/jobsdb"
	uuid "github.com/satori/go.uuid"
	"github.com/segmentio/ksuid"
	"github.com/tidwall/sjson"
)

// Ginkgo tests use the following credentials
// CONFIG_BACKEND_URL=https://api.dev.rudderlabs.com
// CONFIG_BACKEND_TOKEN=1TEeQIJJqpviy5uAbWuxjk1XttY
// USERNAME=srikanth+ginkgo@rudderlabs.com
// PASSWORD=secret123

var sampleEvent = `
	{
		"batch": [
			{
			"anonymousId": "49e4bdd1c280bc00",
			"channel": "android-sdk",
			"destination_props": {
				"AF": {
				"af_uid": "1566363489499-3377330514807116178"
				}
			},
			"context": {
				"app": {
				"build": "1",
				"name": "RudderAndroidClient",
				"namespace": "com.rudderlabs.android.sdk",
				"version": "1.0"
				},
				"device": {
				"id": "49e4bdd1c280bc00",
				"manufacturer": "Google",
				"model": "Android SDK built for x86",
				"name": "generic_x86"
				},
				"locale": "en-US",
				"network": {
				"carrier": "Android"
				},
				"screen": {
				"density": 420,
				"height": 1794,
				"width": 1080
				},
				"traits": {
				"anonymousId": "49e4bdd1c280bc00"
				},
				"user_agent": "Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"
			},
			"event": "Demo Track",
			"integrations": {
				"All": true
			},
			"properties": {
				"label": "Demo Label",
				"category": "Demo Category",
				"value": 5
			},
			"type": "track",
			"originalTimestamp": "2019-08-12T05:08:30.909Z",
			"sentAt": "2019-08-12T05:08:30.909Z"
			}
		]
	}
`

// EventOptsT is the type specifying override options over sample event.json
type EventOptsT struct {
	Integrations map[string]bool
	WriteKey     string
	ID           string
	MessageID    string
	GaVal        int
}

// SendEventRequest sends sample event.json with EventOptionsT overrides to gateway server
func SendEventRequest(options EventOptsT) int {
	if options.Integrations == nil {
		options.Integrations = map[string]bool{
			"All":              false,
			"Google Analytics": true,
		}
	}
	if options.WriteKey == "" {
		options.WriteKey = "1TEg8hHALRE6iIioOsUTAd3Ejq0"
	}
	if options.ID == "" {
		options.ID = ksuid.New().String()
	}
	if options.MessageID == "" {
		options.MessageID = uuid.NewV4().String()
	}

	serverIP := "http://localhost:8080/v1/batch"

	jsonPayload, _ := sjson.Set(sampleEvent, "batch.0.sentAt", time.Now())
	jsonPayload, _ = sjson.Set(jsonPayload, "batch.0.integrations", options.Integrations)
	jsonPayload, _ = sjson.Set(jsonPayload, "batch.0.anonymousId", options.ID)
	jsonPayload, _ = sjson.Set(jsonPayload, "batch.0.messageId", options.MessageID)
	jsonPayload, _ = sjson.Set(jsonPayload, "batch.0.properties.value", options.GaVal)

	req, err := http.NewRequest("POST", serverIP, bytes.NewBuffer([]byte(jsonPayload)))
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(options.WriteKey, "")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// GetTableNamesWithPrefix returns all table names with specified prefix
func GetTableNamesWithPrefix(dbHandle *sql.DB, prefix string) []string {
	//Read the table names from PG
	stmt, err := dbHandle.Prepare(`SELECT tablename
                                        FROM pg_catalog.pg_tables
                                        WHERE schemaname != 'pg_catalog' AND
                                        schemaname != 'information_schema'`)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	tableNames := []string{}
	for rows.Next() {
		var tbName string
		err = rows.Scan(&tbName)
		if err != nil {
			panic(err)
		}
		if strings.HasPrefix(tbName, prefix) {
			tableNames = append(tableNames, tbName)
		}
	}

	return tableNames
}

// GetJobsCount returns count of jobs across all tables with specified prefix
func GetJobsCount(dbHandle *sql.DB, prefix string) int {
	tableNames := GetTableNamesWithPrefix(dbHandle, strings.ToLower(prefix)+"_jobs_")
	count := 0
	for _, tableName := range tableNames {
		var jobsCount int
		dbHandle.QueryRow(fmt.Sprintf(`select count(*) as count from %s;`, tableName)).Scan(&jobsCount)
		count += jobsCount
	}
	return count
}

// GetJobStausCount returns count of job status across all tables with specified prefix
func GetJobStausCount(dbHandle *sql.DB, jobState string, prefix string) int {
	tableNames := GetTableNamesWithPrefix(dbHandle, strings.ToLower(prefix)+"_job_status_")
	count := 0
	for _, tableName := range tableNames {
		var jobsCount int
		dbHandle.QueryRow(fmt.Sprintf(`select count(*) as count from %s where job_state='%s';`, tableName, jobState)).Scan(&jobsCount)
		count += jobsCount
	}
	return count
}

// GetJobs returns jobs (with a limit) across all tables with specified prefix
func GetJobs(dbHandle *sql.DB, prefix string, limit int) []*jobsdb.JobT {
	tableNames := GetTableNamesWithPrefix(dbHandle, strings.ToLower(prefix)+"_jobs_")
	var jobList []*jobsdb.JobT
	for _, tableName := range tableNames {
		rows, err := dbHandle.Query(fmt.Sprintf(`select %[1]s.job_id, %[1]s.uuid, %[1]s.custom_val,
                                               %[1]s.event_payload, %[1]s.created_at, %[1]s.expire_at from %[1]s order by %[1]s.created_at desc limit %v;`, tableName, limit-len(jobList)))
		if err != nil {
			panic(err)
		}
		defer rows.Close()
		for rows.Next() {
			var job jobsdb.JobT
			rows.Scan(&job.JobID, &job.UUID, &job.CustomVal,
				&job.EventPayload, &job.CreatedAt, &job.ExpireAt)
			if len(jobList) < limit {
				jobList = append(jobList, &job)
			}
		}
		if len(jobList) >= limit {
			break
		}
	}
	return jobList
}

// GetJobStatus returns job statuses (with a limit) across all tables with specified prefix
func GetJobStatus(dbHandle *sql.DB, prefix string, limit int, jobState string) []*jobsdb.JobStatusT {
	tableNames := GetTableNamesWithPrefix(dbHandle, strings.ToLower(prefix)+"_job_status_")
	var jobStatusList []*jobsdb.JobStatusT
	for _, tableName := range tableNames {
		rows, err := dbHandle.Query(fmt.Sprintf(`select * from %[1]s where job_state='%s' order by %[1]s.created_at desc limit %v;`, tableName, jobState, limit-len(jobStatusList)))
		if err != nil {
			panic(err)
		}
		defer rows.Close()
		for rows.Next() {
			var jobStatus jobsdb.JobStatusT
			rows.Scan(&jobStatus)
			if len(jobStatusList) < limit {
				jobStatusList = append(jobStatusList, &jobStatus)
			}
		}
		if len(jobStatusList) >= limit {
			break
		}
	}
	return jobStatusList
}

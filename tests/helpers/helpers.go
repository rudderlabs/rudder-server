package helpers

import (
	"bytes"
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/segmentio/ksuid"
	"github.com/tidwall/sjson"
)

var sampleEvent = `
	{
		"writeKey": "#rudderWriteKey#",
		"batch": [
			{
			"rl_message": {
				"rl_anonymous_id": "49e4bdd1c280bc00",
				"rl_channel": "android-sdk",
				"rl_destination_props": {
				"AF": {
					"rl_af_uid": "1566363489499-3377330514807116178"
				}
				},
				"rl_context": {
				"rl_app": {
					"rl_build": "1",
					"rl_name": "RudderAndroidClient",
					"rl_namespace": "com.rudderlabs.android.sdk",
					"rl_version": "1.0"
				},
				"rl_device": {
					"rl_id": "49e4bdd1c280bc00",
					"rl_manufacturer": "Google",
					"rl_model": "Android SDK built for x86",
					"rl_name": "generic_x86"
				},
				"rl_locale": "en-US",
				"rl_network": {
					"rl_carrier": "Android"
				},
				"rl_screen": {
					"rl_density": 420,
					"rl_height": 1794,
					"rl_width": 1080
				},
				"rl_traits": {
					"rl_anonymous_id": "49e4bdd1c280bc00"
				},
				"rl_user_agent": "Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"
				},
				"rl_event": "Demo Track",
				"rl_integrations": {
				"AD": true,
				"AF": true
				},
				"rl_message_id": "1566904375469-1baf3108-647a-4f20-9867-3072056a07f5",
				"rl_properties": {
				"label": "Demo Label",
				"category": "Demo Category",
				"value": 5
				},
				"rl_timestamp": "2019-08-27 11:12:55+0000",
				"rl_type": "track"
			}
			}
		]
	}
`

// EventOptsT is the type specifying override options over sample event.json
type EventOptsT struct {
	Integrations map[string]bool
	WriteKey     string
	ID           string
	GaVal        int
}

// SendEventRequest sends sample event.json with EventOptionsT overrides to gateway server
func SendEventRequest(options EventOptsT) int {
	if options.Integrations == nil {
		options.Integrations = map[string]bool{
			"All": false,
			"GA":  true,
		}
	}
	if options.WriteKey == "" {
		options.WriteKey = "1REVKlIoVwDAIwv4WuMxYexaJ5w"
	}
	if options.ID == "" {
		options.ID = ksuid.New().String()
	}

	serverIP := "http://localhost:8080/events"

	jsonPayload, _ := sjson.Set(sampleEvent, "writeKey", options.WriteKey)
	jsonPayload, _ = sjson.Set(jsonPayload, "sent_at", time.Now())
	jsonPayload, _ = sjson.Set(jsonPayload, "batch.0.rl_message.rl_integrations", options.Integrations)
	jsonPayload, _ = sjson.Set(jsonPayload, "batch.0.rl_message.rl_anonymous_id", options.ID)
	jsonPayload, _ = sjson.Set(jsonPayload, "batch.0.rl_message.rl_traits.rl_anonymous_id", options.ID)
	jsonPayload, _ = sjson.Set(jsonPayload, "batch.0.rl_message.rl_properties.value", options.GaVal)

	req, err := http.NewRequest("POST", serverIP, bytes.NewBuffer([]byte(jsonPayload)))
	req.Header.Set("Content-Type", "application/json")

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
	misc.AssertError(err)
	defer stmt.Close()

	rows, err := stmt.Query()
	defer rows.Close()
	misc.AssertError(err)

	tableNames := []string{}
	for rows.Next() {
		var tbName string
		err = rows.Scan(&tbName)
		misc.AssertError(err)
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

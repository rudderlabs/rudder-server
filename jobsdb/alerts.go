package jobsdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/services/stats"
)

type jobByState struct {
	workspace   string
	state       string
	customVal   string
	destination string
	count       sql.NullInt64
}

func (jd *HandleT) PendingJobCountStat(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(pendingJobCountStatTime):
			func() {
				jobCountMap := make(map[string]int)
				jd.dsListLock.RLock()
				defer jd.dsListLock.RUnlock()
				dsList := jd.getDSList(false)

				for _, ds := range dsList {
					jobCounts := jd.PendingJobCountStatByDS(ds)
					for _, jobCount := range jobCounts {
						key := strings.Join([]string{jobCount.customVal, jobCount.state, jobCount.workspace, jobCount.destination}, `||<<>>||`)
						if _, ok := jobCountMap[key]; !ok {
							jobCountMap[key] = int(jobCount.count.Int64)
						} else {
							jobCountMap[key] += int(jobCount.count.Int64)
						}
					}
				}

				for key := range jobCountMap {
					count := jobCountMap[key]
					tags := strings.Split(key, `||<<>>||`)
					pendingJobCountStat := stats.NewTaggedStat("pending_jobs", stats.GaugeType, stats.Tags{
						"tablePrefix": jd.tablePrefix,
						"customVal":   tags[0],
						"jobState":    tags[1],
						"workspace":   tags[2],
						"destination": tags[3],
					})
					pendingJobCountStat.Gauge(count)
				}
			}()
		}
	}
}

func (jd *HandleT) PendingJobCountStatByDS(ds dataSetT) []*jobByState {
	sqlStatement := fmt.Sprintf(`WITH joined AS (
		SELECT 
			j.job_id as jobID, 
			j.custom_val as customVal,
			j.workspace_id as workspace,
			s.id as statusID, 
			s.job_state as jobState, 
			j.parameters as parameters
		FROM %s j left join %s s on j.job_id = s.job_id
	),`, ds.JobTable, ds.JobStatusTable) + fmt.Sprintf(`x as (
		SELECT *, ROW_NUMBER() OVER(PARTITION BY joined.jobID 
									ORDER BY joined.statusID DESC) AS rank
		FROM joined
	),
	y as (
		SELECT * FROM x WHERE rank = 1
	)
	SELECT 
		count(*) as count,
		workspace,
		customVal, 
		COALESCE(jobState, 'not_picked_yet') as status, 
		COALESCE(parameters->>'destination_id', 'gateway') as dest
	FROM y WHERE %s 
	GROUP by workspace, customVal, jobState, dest`, "("+constructQuery(jd, "jobState", getNonTerminalStates(), "OR")+")")

	// jd.logger.Info(sqlStatement)
	stmt, err := jd.dbHandle.Prepare(sqlStatement)
	jd.assertError(err)
	defer stmt.Close()

	var rows *sql.Rows
	rows, err = stmt.Query()
	jd.assertError(err)
	defer rows.Close()

	var jobCountList []*jobByState
	for rows.Next() {
		var jobCount jobByState
		err := rows.Scan(&jobCount.count, &jobCount.workspace, &jobCount.customVal, &jobCount.state, &jobCount.destination)
		jd.assertError(err)
		jobCountList = append(jobCountList, &jobCount)
	}

	return jobCountList
}

func getNonTerminalStates() (terminalStates []string) {
	for _, js := range jobStates {
		if !js.isTerminal {
			terminalStates = append(terminalStates, js.State)
		}
	}
	return
}

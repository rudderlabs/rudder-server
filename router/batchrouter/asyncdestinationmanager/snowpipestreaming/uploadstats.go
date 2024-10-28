package snowpipestreaming

import (
	"fmt"
	"net/http"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

// GetUploadStats returns the status of the uploads for the snowpipe streaming destination.
// It returns the status of the uploads for the given job IDs.
// If any of the uploads have failed, it returns the reason for the failure.
func (m *Manager) GetUploadStats(input common.GetUploadStatsInput) common.GetUploadStatsResponse {
	m.logger.Infon("Getting import stats for snowpipe streaming destination")

	var infos []*importInfo
	err := json.Unmarshal([]byte(input.FailedJobURLs), &infos)
	if err != nil {
		return common.GetUploadStatsResponse{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("failed to unmarshal failed job urls: %w", err).Error(),
		}
	}

	succeededTables, failedTables := make(map[string]struct{}), make(map[string]*importInfo)
	for _, info := range infos {
		if info.Failed {
			failedTables[info.Table] = info
		} else {
			succeededTables[info.Table] = struct{}{}
		}
	}

	var (
		succeededJobIDs  []int64
		failedJobIDs     []int64
		failedJobReasons = make(map[int64]string)
	)
	for _, job := range input.ImportingList {
		tableName := gjson.GetBytes(job.EventPayload, "metadata.table").String()
		if _, ok := succeededTables[tableName]; ok {
			succeededJobIDs = append(succeededJobIDs, job.JobID)
		}
		if info, ok := failedTables[tableName]; ok {
			failedJobIDs = append(failedJobIDs, job.JobID)
			failedJobReasons[job.JobID] = info.Reason
		}
	}
	return common.GetUploadStatsResponse{
		StatusCode: http.StatusOK,
		Metadata: common.EventStatMeta{
			FailedKeys:    failedJobIDs,
			SucceededKeys: succeededJobIDs,
			FailedReasons: failedJobReasons,
		},
	}
}

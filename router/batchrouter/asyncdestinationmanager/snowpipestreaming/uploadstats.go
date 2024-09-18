package snowpipestreaming

import (
	"fmt"
	"net/http"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

func (m *Manager) GetUploadStats(input common.GetUploadStatsInput) common.GetUploadStatsResponse {
	m.logger.Infon("Getting upload stats for snowpipe streaming destination")

	var infos []uploadInfo
	err := json.Unmarshal([]byte(input.FailedJobURLs), &infos)
	if err != nil {
		m.logger.Warnn("Failed to unmarshal failed job urls", obskit.Error(err))
		return common.GetUploadStatsResponse{
			StatusCode: 500,
			Error:      fmt.Errorf("failed to unmarshal failed job urls: %v", err).Error(),
		}
	}

	var (
		succeededTables map[string]uploadInfo
		failedTables    map[string]uploadInfo
	)

	for _, info := range infos {
		if info.Failed {
			failedTables[info.Table] = info
		} else {
			succeededTables[info.Table] = info
		}
	}

	var (
		succeededJobIDs  []int64
		failedJobIDs     []int64
		failedJobReasons map[int64]string
	)

	for _, job := range input.ImportingList {
		tableName := gjson.GetBytes(job.EventPayload, "metadata.table").String()
		if _, ok := succeededTables[tableName]; ok {
			succeededJobIDs = append(succeededJobIDs, job.JobID)
			continue
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

package processor

import (
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func (proc *Handle) getDroppedJobs(response transformer.Response, eventsToTransform []transformer.TransformerEvent) []*jobsdb.JobT {
	// each messageID is one event when sending to the transformer
	inputMessageIDs := lo.Map(eventsToTransform, func(e transformer.TransformerEvent, _ int) string {
		return e.Metadata.MessageID
	})

	// in transformer response, multiple messageIDs could be batched together
	successFullMessageIDs := make([]string, 0)
	lo.ForEach(response.Events, func(e transformer.TransformerResponse, _ int) {
		successFullMessageIDs = append(successFullMessageIDs, e.Metadata.GetMessagesIDs()...)
	})
	// for failed as well
	failedMessageIDs := make([]string, 0)
	lo.ForEach(response.FailedEvents, func(e transformer.TransformerResponse, _ int) {
		failedMessageIDs = append(failedMessageIDs, e.Metadata.GetMessagesIDs()...)
	})
	// the remainder of the messageIDs are those that are dropped
	// we get jobs for those dropped messageIDs - for rsources_stats_collector
	droppedMessageIDs, _ := lo.Difference(inputMessageIDs, append(successFullMessageIDs, failedMessageIDs...))
	droppedMessageIDKeys := lo.SliceToMap(droppedMessageIDs, func(m string) (string, struct{}) { return m, struct{}{} })
	droppedJobs := make([]*jobsdb.JobT, 0)
	for _, e := range eventsToTransform {
		if _, ok := droppedMessageIDKeys[e.Metadata.MessageID]; ok {
			params := struct {
				SourceJobRunID  string      `json:"source_job_run_id"`
				SourceTaskRunID string      `json:"source_task_run_id"`
				SourceID        string      `json:"source_id"`
				DestinationID   string      `json:"destination_id"`
				RecordID        interface{} `json:"record_id"`
			}{
				SourceJobRunID:  e.Metadata.SourceJobRunID,
				SourceTaskRunID: e.Metadata.SourceTaskRunID,
				SourceID:        e.Metadata.SourceID,
				DestinationID:   e.Metadata.DestinationID,
				RecordID:        e.Metadata.RecordID,
			}
			marshalledParams, err := jsonfast.Marshal(params)
			if err != nil {
				proc.logger.Errorf("[Processor] Failed to marshal parameters. Parameters: %v", params)
				marshalledParams = []byte(`{"error": "Processor failed to marshal params"}`)
			}

			droppedJobs = append(droppedJobs, &jobsdb.JobT{
				UUID:         misc.FastUUID(),
				EventPayload: []byte(`{}`),
				Parameters:   marshalledParams,
				CreatedAt:    time.Now(),
				ExpireAt:     time.Now(),
				CustomVal:    e.Metadata.DestinationType,
				UserID:       e.Metadata.RudderID,
				WorkspaceId:  e.Metadata.WorkspaceID,
			})
		}
	}
	return droppedJobs
}

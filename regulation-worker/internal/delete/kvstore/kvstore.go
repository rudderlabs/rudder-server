package kvstore

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/kvstoremanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	supportedDestinations = []string{"REDIS"}
	pkgLogger             = logger.NewLogger().Child("kvstore")
)

type KVDeleteManager struct {
}

func (kv *KVDeleteManager) GetSupportedDestinations() []string {

	return supportedDestinations
}

func (kv *KVDeleteManager) Delete(ctx context.Context, job model.Job, destConfig map[string]interface{}, destName string) model.JobStatus {
	pkgLogger.Debugf("deleting job: %v", job, " from kvstore")
	kvm := kvstoremanager.New(destName, destConfig)
	var err error
	fileCleaningTime := stats.NewTaggedStat("file_cleaning_time", stats.TimerType, stats.Tags{"jobId": fmt.Sprintf("%d", job.ID), "workspaceId": job.WorkspaceID, "destType": "kvstore", "destName": destName})
	fileCleaningTime.Start()
	defer fileCleaningTime.End()
	for _, user := range job.UserAttributes {
		err = kvm.DeleteKey(user.UserID)
		if err != nil {
			pkgLogger.Errorf("failed to delete user: %v", user.UserID, "with error: %v", err)
			return model.JobStatusFailed
		}
	}

	pkgLogger.Debugf("deletion successful")
	return model.JobStatusComplete
}

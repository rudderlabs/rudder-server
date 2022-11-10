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

type KVDeleteManager struct{}

func (*KVDeleteManager) GetSupportedDestinations() []string {
	return supportedDestinations
}

func (*KVDeleteManager) Delete(_ context.Context, job model.Job, destDetail model.Destination) model.JobStatus {
	destConfig := destDetail.Config
	destName := destDetail.Name

	pkgLogger.Debugf("deleting job: %v", job, " from kvstore")
	kvm := kvstoremanager.New(destName, destConfig)
	var err error
	fileCleaningTime := stats.Default.NewTaggedStat("file_cleaning_time", stats.TimerType, stats.Tags{"jobId": fmt.Sprintf("%d", job.ID), "workspaceId": job.WorkspaceID, "destType": "kvstore", "destName": destName})
	fileCleaningTime.Start()
	defer fileCleaningTime.End()
	for _, user := range job.Users {
		key := fmt.Sprintf("user:%s", user.ID)
		err = kvm.DeleteKey(key)
		if err != nil {
			pkgLogger.Errorf("failed to delete user: %s with error: %w", user.ID, err)
			return model.JobStatusFailed
		}
	}

	pkgLogger.Debugf("deletion successful")
	return model.JobStatusComplete
}

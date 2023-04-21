package kvstore

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/kvstoremanager"
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
	fileCleaningTime := stats.Default.NewTaggedStat(
		"regulation_worker_cleaning_time",
		stats.TimerType,
		stats.Tags{
			"destinationId": job.DestinationID,
			"workspaceId":   job.WorkspaceID,
			"jobType":       "kvstore",
		})
	defer fileCleaningTime.RecordDuration()()
	for _, user := range job.Users {
		key := fmt.Sprintf("user:%s", user.ID)
		err = kvm.DeleteKey(key)
		if err != nil {
			pkgLogger.Errorf("failed to delete user: %s with error: %v", user.ID, err)
			return model.JobStatus{Status: model.JobStatusFailed, Error: err}
		}
	}

	pkgLogger.Debugf("deletion successful")
	return model.JobStatus{Status: model.JobStatusComplete}
}

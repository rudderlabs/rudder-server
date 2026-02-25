package kvstore

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
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

func (*KVDeleteManager) Delete(_ context.Context, job model.Job, destination *backendconfig.DestinationT) model.JobStatus {
	destConfig := destination.Config
	destName := destination.DestinationDefinition.Name

	pkgLogger.Debugn("deleting from kvstore",
		logger.NewIntField("jobID", int64(job.ID)),
		obskit.WorkspaceID(job.WorkspaceID),
		obskit.DestinationID(job.DestinationID))
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
			pkgLogger.Errorn("failed to delete user",
				logger.NewStringField("userID", user.ID),
				obskit.Error(err))
			return model.JobStatus{Status: model.JobStatusFailed, Error: err}
		}
	}

	pkgLogger.Debugn("deletion successful")
	return model.JobStatus{Status: model.JobStatusComplete}
}

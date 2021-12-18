package kvstore

import (
	"context"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/kvstoremanager"
)

var supportedDestinations = []string{"REDIS"}

type KVDeleteManager struct{}

func (kv *KVDeleteManager) GetSupportedDestinations() []string {
	return supportedDestinations
}

func (kv *KVDeleteManager) Delete(ctx context.Context, job model.Job, destConfig map[string]interface{}, destName string) model.JobStatus {
	kvm := kvstoremanager.New(destName, destConfig)
	var err error
	for _, user := range job.UserAttributes {
		err = kvm.DeleteKey(user.UserID)
		if err != nil {
			return model.JobStatusFailed
		}
	}
	return model.JobStatusComplete
}

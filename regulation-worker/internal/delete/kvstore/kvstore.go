package kvstore

import (
	"context"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/kvstoremanager"
)

type KVDeleteManager struct {
	KVStoreManager kvstoremanager.KVStoreManager
}

func (kv *KVDeleteManager) Delete(ctx context.Context, job model.Job, destConfig map[string]interface{}, destName string) model.JobStatus {

	kv.KVStoreManager.Connect()
	var err error
	for _, user := range job.UserAttributes {
		err = kv.KVStoreManager.DeleteKey(user.UserID)
		if err != nil {
			return model.JobStatusFailed
		}
	}
	return model.JobStatusComplete
}

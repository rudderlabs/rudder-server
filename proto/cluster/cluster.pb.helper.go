package proto

import (
	"github.com/google/uuid"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

func (x *JobsBatchChunk) GetJobsdbJobs() ([]*jobsdb.JobT, error) {
	res := make([]*jobsdb.JobT, 0, len(x.Jobs))
	for _, j := range x.Jobs {
		u, err := uuid.FromBytes(j.Uuid)
		if err != nil {
			return nil, err
		}
		res = append(res, &jobsdb.JobT{
			UUID:         u,
			JobID:        j.JobID,
			UserID:       j.UserId,
			CreatedAt:    j.CreatedAt.AsTime(),
			ExpireAt:     j.ExpireAt.AsTime(),
			CustomVal:    j.CustomVal,
			EventCount:   int(j.EventCount),
			EventPayload: j.EventPayload,
			Parameters:   j.Parameters,
			WorkspaceId:  j.WorkspaceId,
			PartitionID:  j.PartitionId,
		})
	}
	return res, nil
}

func JobFromJobsdbJob(j *jobsdb.JobT, _ int) *Job {
	return &Job{
		Uuid:         j.UUID[:],
		JobID:        j.JobID,
		UserId:       j.UserID,
		CreatedAt:    timestamppb.New(j.CreatedAt),
		ExpireAt:     timestamppb.New(j.ExpireAt),
		CustomVal:    j.CustomVal,
		EventCount:   int64(j.EventCount),
		EventPayload: j.EventPayload,
		Parameters:   j.Parameters,
		WorkspaceId:  j.WorkspaceId,
		PartitionId:  j.PartitionID,
	}
}

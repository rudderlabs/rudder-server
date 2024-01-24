package forwarder

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/internal/pulsar"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/schema-forwarder/internal/testdata"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

func Test_JobsForwarder(t *testing.T) {
	conf := config.New()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pulsarContainer := PulsarResource(t)

	conf.Set("Pulsar.Client.url", pulsarContainer.URL)
	conf.Set("SchemaForwarder.loopSleepTime", time.Millisecond)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(gomock.NewController(t))

	postgres, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	t.Setenv("JOBS_DB_PORT", postgres.Port)
	t.Setenv("JOBS_DB_USER", postgres.User)
	t.Setenv("JOBS_DB_DB_NAME", postgres.Database)
	t.Setenv("JOBS_DB_PASSWORD", postgres.Password)
	schemasDB := jobsdb.NewForReadWrite(
		"test_event_schema",
	)
	err = schemasDB.Start()
	require.NoError(t, err)
	defer schemasDB.TearDown()

	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{testdata.SampleWorkspaceID: testdata.SampleBackendConfig}, Topic: string(topic)}
			close(ch)
			return ch
		})

	client, err := pulsar.NewClient(conf)
	require.NoError(t, err)
	jf := NewJobsForwarder(func(error) {}, schemasDB, &client, conf, mockBackendConfig, logger.NOP, stats.Default)
	require.NotNil(t, jf)
	require.NoError(t, jf.Start())
	defer jf.Stop()

	t.Run("jobs for a valid source should succeed", func(t *testing.T) {
		generateJobs := func(numOfJob int) []*jobsdb.JobT {
			customVal := "MOCKDS"
			js := make([]*jobsdb.JobT, numOfJob)
			for i := 0; i < numOfJob; i++ {
				js[i] = &jobsdb.JobT{
					Parameters:   []byte(`{"batch_id":1,"source_id":"enabled-source","source_job_run_id":""}`),
					EventPayload: []byte(`{"testKey":"testValue"}`),
					UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
					UUID:         uuid.New(),
					CustomVal:    customVal,
					EventCount:   1,
					WorkspaceId:  "test_workspace",
				}
			}
			return js
		}
		jobs := generateJobs(10)
		err = schemasDB.Store(context.Background(), jobs)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			jobs, err := schemasDB.GetSucceeded(context.Background(), jobsdb.GetQueryParams{
				JobsLimit: 10,
			})
			require.NoError(t, err)
			return len(jobs.Jobs) == 10
		}, 20*time.Second, 100*time.Millisecond)
	})

	t.Run("jobs for an invalid source should be aborted", func(t *testing.T) {
		generateJobs := func(numOfJob int) []*jobsdb.JobT {
			customVal := "MOCKDS"
			js := make([]*jobsdb.JobT, numOfJob)
			for i := 0; i < numOfJob; i++ {
				js[i] = &jobsdb.JobT{
					Parameters:   []byte(`{"batch_id":1,"source_id":"invalid","source_job_run_id":""}`),
					EventPayload: []byte(`{"testKey":"testValue"}`),
					UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
					UUID:         uuid.New(),
					CustomVal:    customVal,
					EventCount:   1,
					WorkspaceId:  "test_workspace",
				}
			}
			return js
		}
		jobs := generateJobs(10)
		err = schemasDB.Store(context.Background(), jobs)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			jobs, err := schemasDB.GetAborted(context.Background(), jobsdb.GetQueryParams{
				JobsLimit: 10,
			})
			require.NoError(t, err)
			return len(jobs.Jobs) == 10
		}, 20*time.Second, 100*time.Millisecond)
	})
}

// PulsarResource returns a pulsar container resource
func PulsarResource(t *testing.T) *resource.PulsarResource {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pulsarContainer, err := resource.SetupPulsar(pool, t)
	require.NoError(t, err)
	return pulsarContainer
}

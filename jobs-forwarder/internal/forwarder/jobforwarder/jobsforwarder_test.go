package jobforwarder

import (
	"context"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/stretchr/testify/require"
)

func Test_JobsForwarder(t *testing.T) {
	g, ctx := errgroup.WithContext(context.Background())
	conf := config.New()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	topic := "test-topic"
	pulsarContainer := PulsarResource(t)

	conf.Set("Pulsar.Client.url", pulsarContainer.URL)
	conf.Set("Pulsar.Producer.topic", topic)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(gomock.NewController(t))

	jobsdb.Init()
	jobsdb.Init2()
	postgres, err := resource.SetupPostgres(pool, t)
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
			ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{SampleWorkspaceID: SampleBackendConfig}, Topic: string(topic)}
			close(ch)
			return ch
		})

	jf, err := New(ctx, g, schemasDB, conf, transientsource.NewEmptyService(), mockBackendConfig, logger.NOP)
	require.NoError(t, err)
	require.NotNil(t, jf)
	jf.Start()
	defer jf.Stop()
	generateJobs := func(numOfJob int) []*jobsdb.JobT {
		customVal := "MOCKDS"
		js := make([]*jobsdb.JobT, numOfJob)
		for i := 0; i < numOfJob; i++ {
			js[i] = &jobsdb.JobT{
				Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
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
	err = schemasDB.Store(ctx, jobs)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		jobs, err := schemasDB.GetProcessed(ctx, jobsdb.GetQueryParamsT{
			StateFilters: []string{jobsdb.Succeeded.State},
			JobsLimit:    10,
		})
		require.NoError(t, err)
		return len(jobs.Jobs) == 10
	}, 30*time.Second, 5*time.Second)
}

// PulsarResource returns a pulsar container resource
func PulsarResource(t *testing.T) *resource.PulsarResource {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pulsarContainer, err := resource.SetupPulsar(pool, t)
	require.NoError(t, err)
	return pulsarContainer
}

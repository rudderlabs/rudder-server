package transientsource

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/gomega"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mock_backendconfig "github.com/rudderlabs/rudder-server/mocks/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

func Test_Service_Apply(t *testing.T) {
	RegisterTestingT(t)

	service := NewStaticService([]string{"one"})

	Expect(service.Apply("one")).To(Equal(true))
	Expect(service.Apply("two")).To(Equal(false))
}

func Test_Service_ApplyJob(t *testing.T) {
	RegisterTestingT(t)

	service := NewStaticService([]string{"one"})

	job := &jobsdb.JobT{
		Parameters: json.RawMessage(`{"source_id": "one"}`),
	}

	Expect(service.ApplyJob(job)).To(Equal(true))

	job = &jobsdb.JobT{
		Parameters: json.RawMessage(`{"source_id": "two"}`),
	}
	Expect(service.ApplyJob(job)).To(Equal(false))
}

func Test_Service_ApplyParams(t *testing.T) {
	RegisterTestingT(t)

	service := NewStaticService([]string{"one"})

	params := json.RawMessage(`{"source_id": "one"}`)

	Expect(service.ApplyParams(params)).To(Equal(true))

	params = json.RawMessage(`{"source_id": "two"}`)

	Expect(service.ApplyParams(params)).To(Equal(false))
}

func Test_SourceIdsSupplier_Normal_Flow(t *testing.T) {
	RegisterTestingT(t)
	ctrl := gomock.NewController(t)
	config := mock_backendconfig.NewMockBackendConfig(ctrl)

	var ch chan pubsub.DataEvent

	var ready sync.WaitGroup
	ready.Add(2)

	var gotSourceIds sync.WaitGroup
	gotSourceIds.Add(1)

	config.EXPECT().Subscribe(
		gomock.Any(),
		gomock.Eq(backendconfig.TopicBackendConfig),
	).
		Do(func(channel chan pubsub.DataEvent, topic backendconfig.Topic) {
			ch = channel
			ready.Done()
		})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Given I have a service reading from the backend
	service := NewService(ctx, config)
	sourceIdsSupplier := service.SourceIdsSupplier()
	var sourceIds []string

	go func() {
		ready.Done()
		sourceIds = sourceIdsSupplier()
		gotSourceIds.Done()
	}()

	// When the config backend has not published any event yet
	ready.Wait()

	// Then source ids are still empty
	Expect(sourceIds).To(BeEmpty())

	// When the config backend publishes an event with two skipped sources
	ch <- pubsub.DataEvent{
		Data: backendconfig.ConfigT{
			Sources: []backendconfig.SourceT{
				{
					ID:     "one",
					Config: map[string]interface{}{"transient": true},
				},
				{
					ID:     "two",
					Config: map[string]interface{}{"transient": true},
				},
			},
		},
		Topic: string(backendconfig.TopicBackendConfig),
	}
	gotSourceIds.Wait()
	// Then source ids will contain the two expected elements
	Expect(sourceIds).To(Equal([]string{"one", "two"}))
}

func Test_SourceIdsSupplier_Context_Cancelled(t *testing.T) {
	RegisterTestingT(t)
	ctrl := gomock.NewController(t)
	config := mock_backendconfig.NewMockBackendConfig(ctrl)

	var ready sync.WaitGroup
	ready.Add(2)

	var gotSourceIds sync.WaitGroup
	gotSourceIds.Add(1)

	config.EXPECT().Subscribe(
		gomock.Any(),
		gomock.Eq(backendconfig.TopicBackendConfig),
	).
		Do(func(channel chan pubsub.DataEvent, topic backendconfig.Topic) {
			ready.Done()
		})
	ctx, cancel := context.WithCancel(context.Background())

	// Given I have a service reading from the backend
	service := NewService(ctx, config)
	sourceIdsSupplier := service.SourceIdsSupplier()
	var sourceIds []string

	go func() {
		ready.Done()
		sourceIds = sourceIdsSupplier()
		gotSourceIds.Done()
	}()

	// When the config backend has not published any event yet
	ready.Wait()

	// Then source ids are still empty
	Expect(sourceIds).To(BeEmpty())

	// When the context is canceled
	cancel()

	// And source Ids are received from the provider
	gotSourceIds.Wait()

	// Then the source ids remain empty
	Expect(sourceIds).To(BeEmpty())
}

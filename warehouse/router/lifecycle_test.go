package router

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestLifecycle(t *testing.T) {
	t.Run("normal lifecycle", func(t *testing.T) {
		ps := &configPubSub{}
		runningDestTypes := []string{}

		wg := sync.WaitGroup{}

		lm := NewLifecycleManager(ps, func(ctx context.Context, destType string) error {
			runningDestTypes = append(runningDestTypes, destType)
			wg.Done()
			<-ctx.Done()
			return nil
		})

		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan error, 1)
		go func() {
			defer close(done)
			done <- lm.Run(ctx)
		}()

		wg.Add(1)
		ps.PublishConfig(configGen(t, whutils.POSTGRES, whutils.POSTGRES, "CLOUD_DESTINATION"))
		wg.Wait()
		require.Equal(t, []string{whutils.POSTGRES}, runningDestTypes)

		wg.Add(1)
		ps.PublishConfig(configGen(t, whutils.POSTGRES, whutils.SNOWFLAKE))
		wg.Wait()
		require.Equal(t, []string{whutils.POSTGRES, whutils.SNOWFLAKE}, runningDestTypes)

		cancel()
		err := <-done
		require.NoError(t, err)
	})

	t.Run("error in handler", func(t *testing.T) {
		ps := &configPubSub{}
		wg := sync.WaitGroup{}
		triggerErr := make(chan error)

		lm := NewLifecycleManager(ps, func(ctx context.Context, destType string) error {
			wg.Done()

			return <-triggerErr
		})

		done := make(chan error, 1)
		go func() {
			defer close(done)
			done <- lm.Run(context.Background())
		}()

		t.Log("setup a handler")
		wg.Add(1)
		ps.PublishConfig(configGen(t, whutils.POSTGRES))
		wg.Wait()

		triggerErr <- fmt.Errorf("some error")

		require.Equal(t, fmt.Errorf("some error"), <-done)
	})
}

type configPubSub struct {
	ps pubsub.PublishSubscriber
}

func (cp *configPubSub) WatchConfig(ctx context.Context) <-chan map[string]backendconfig.ConfigT {
	chIn := cp.ps.Subscribe(ctx, "test_config")
	chOut := make(chan map[string]backendconfig.ConfigT)
	go func() {
		for data := range chIn {
			input := data.Data.(map[string]backendconfig.ConfigT)
			chOut <- input
		}
		close(chOut)
	}()

	return chOut
}

func (cp *configPubSub) PublishConfig(config map[string]backendconfig.ConfigT) {
	cp.ps.Publish("test_config", config)
}

func configGen(t testing.TB, destType ...string) map[string]backendconfig.ConfigT {
	t.Helper()

	dsts := []backendconfig.DestinationT{}
	for _, dt := range destType {
		dsts = append(dsts, backendconfig.DestinationT{
			ID:      uuid.NewString(),
			Enabled: true,
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				Name: dt,
			},
		})
	}

	wID := uuid.NewString()
	return map[string]backendconfig.ConfigT{
		wID: {
			WorkspaceID: wID,
			Sources: []backendconfig.SourceT{
				{
					ID:           uuid.NewString(),
					Enabled:      true,
					Destinations: dsts,
				},
			},
		},
	}
}

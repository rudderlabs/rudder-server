package router

import (
	"context"
	"slices"

	"golang.org/x/sync/errgroup"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type configWatcher interface {
	WatchConfig(ctx context.Context) <-chan map[string]backendconfig.ConfigT
}

type LifecycleManager struct {
	watcher configWatcher
	handler func(ctx context.Context, destType string) error

	destTypes map[string]struct{}
}

func NewLifecycleManager(
	watcher configWatcher,
	onDestType func(ctx context.Context, destType string) error,
) *LifecycleManager {
	return &LifecycleManager{
		watcher:   watcher,
		handler:   onDestType,
		destTypes: make(map[string]struct{}),
	}
}

func (lm *LifecycleManager) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	ch := lm.watcher.WatchConfig(ctx)
	g.Go(func() error {
		for configData := range ch {
			destTypes := lm.newDestTypes(configData)
			for _, d := range destTypes {
				g.Go(func() error {
					return lm.handler(ctx, d)
				})
			}
		}
		return nil
	})

	return g.Wait()
}

func (lm *LifecycleManager) newDestTypes(
	configMap map[string]backendconfig.ConfigT,
) []string {
	var newDestTypes []string

	for _, wConfig := range configMap {
		for _, source := range wConfig.Sources {
			for _, destination := range source.Destinations {

				destType := destination.DestinationDefinition.Name

				if !slices.Contains(warehouseutils.WarehouseDestinations, destType) {
					continue
				}

				_, ok := lm.destTypes[destType]
				if ok {
					continue
				}

				lm.destTypes[destType] = struct{}{}

				newDestTypes = append(newDestTypes, destType)
			}
		}
	}

	return newDestTypes
}

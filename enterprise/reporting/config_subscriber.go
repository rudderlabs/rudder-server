package reporting

import (
	"context"
	"sync"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

type destDetail struct {
	destinationDefinitionID string
	destType                string // destination definition name
}

type configSubscriber struct {
	init     chan struct{}
	onceInit sync.Once

	log logger.Logger

	backendConfigMu           sync.RWMutex // protects the following
	workspaceID               string
	workspaceIDForSourceIDMap map[string]string
	destinationIDMap          map[string]destDetail
	piiReportingSettings      map[string]bool
}

func newConfigSubscriber(log logger.Logger) *configSubscriber {
	return &configSubscriber{
		init:                      make(chan struct{}),
		log:                       log,
		workspaceIDForSourceIDMap: make(map[string]string),
		destinationIDMap:          make(map[string]destDetail),
		piiReportingSettings:      make(map[string]bool),
	}
}

func (cs *configSubscriber) Subscribe(
	ctx context.Context,
	bcConfig backendconfig.BackendConfig,
) {
	cs.log.Infow("Subscribing to backend config changes")

	defer cs.onceInit.Do(func() {
		close(cs.init)
	})

	if ctx.Err() != nil {
		return
	}

	ch := bcConfig.Subscribe(ctx, backendconfig.TopicBackendConfig)

	for c := range ch {
		conf := c.Data.(map[string]backendconfig.ConfigT)

		workspaceIDForSourceIDMap := make(map[string]string)
		destinationIDMap := make(map[string]destDetail)
		piiReportingSettings := make(map[string]bool)

		var newWorkspaceID string

		for workspaceID, wConfig := range conf {
			newWorkspaceID = workspaceID

			for _, source := range wConfig.Sources {
				workspaceIDForSourceIDMap[source.ID] = workspaceID

				// Reduce to destination detail based on destinationID
				destinationIDMap = lo.Reduce(
					source.Destinations,
					func(agg map[string]destDetail, destination backendconfig.DestinationT, _ int) map[string]destDetail {
						agg[destination.ID] = destDetail{
							destinationDefinitionID: destination.DestinationDefinition.ID,
							destType:                destination.DestinationDefinition.Name,
						}
						return agg
					},
					destinationIDMap,
				)
			}
			piiReportingSettings[workspaceID] = wConfig.Settings.DataRetention.DisableReportingPII
		}
		if len(conf) > 1 {
			newWorkspaceID = ""
		}

		cs.backendConfigMu.Lock()
		cs.workspaceID = newWorkspaceID
		cs.workspaceIDForSourceIDMap = workspaceIDForSourceIDMap
		cs.destinationIDMap = destinationIDMap
		cs.piiReportingSettings = piiReportingSettings
		cs.backendConfigMu.Unlock()

		cs.onceInit.Do(func() {
			close(cs.init)
		})
	}
}

// Wait blocks until the config subscriber is initialized
func (cs *configSubscriber) Wait() {
	<-cs.init
}

// WorkspaceID returns the workspace ID
func (cs *configSubscriber) WorkspaceID() string {
	cs.Wait()

	cs.backendConfigMu.RLock()
	defer cs.backendConfigMu.RUnlock()

	return cs.workspaceID
}

// WorkspaceIDFromSource returns the workspace ID for the given source ID
func (cs *configSubscriber) WorkspaceIDFromSource(sourceID string) string {
	cs.Wait()

	cs.backendConfigMu.RLock()
	defer cs.backendConfigMu.RUnlock()

	return cs.workspaceIDForSourceIDMap[sourceID]
}

// GetDestDetail returns the destination detail for the given destination ID
func (cs *configSubscriber) GetDestDetail(destID string) destDetail {
	cs.Wait()

	cs.backendConfigMu.RLock()
	defer cs.backendConfigMu.RUnlock()

	return cs.destinationIDMap[destID]
}

// IsPIIReportingDisabled returns true if PII reporting is disabled for the given workspace
func (cs *configSubscriber) IsPIIReportingDisabled(workspaceID string) bool {
	cs.Wait()

	cs.backendConfigMu.RLock()
	defer cs.backendConfigMu.RUnlock()

	return cs.piiReportingSettings[workspaceID]
}

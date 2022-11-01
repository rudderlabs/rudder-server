package suppression

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"

	"github.com/rudderlabs/rudder-server/utils/logger"

	"github.com/rudderlabs/rudder-server/rruntime"
)

// SuppressRegulationHandler is a handle to this object
type SuppressRegulationHandler struct {
	Client                          *http.Client
	RegulationBackendURL            string
	RegulationsPollInterval         time.Duration
	ID                              identity.Identifier
	userSpecificSuppressedSourceMap map[string]map[string]sourceFilter
	regulationsSubscriberLock       sync.RWMutex
	suppressAPIToken                string
	pageSize                        string
	once                            sync.Once
}

type sourceFilter struct {
	all      bool
	specific map[string]struct{}
}

var pkgLogger logger.Logger

type apiResponse struct {
	SourceRegulations []sourceRegulation `json:"items"`
	Token             string             `json:"token"`
}

type sourceRegulation struct {
	Canceled    bool     `json:"canceled"`
	WorkspaceID string   `json:"workspaceId"`
	UserID      string   `json:"userId"`
	SourceIDs   []string `json:"sourceIds"`
}

func (suppressUser *SuppressRegulationHandler) setup(ctx context.Context) {
	suppressUser.RegulationBackendURL = configBackendURL
	switch suppressUser.ID.Type() {
	case deployment.DedicatedType:
		suppressUser.RegulationBackendURL += fmt.Sprintf("/dataplane/workspaces/%s/regulations/suppressions", suppressUser.ID.ID())
	case deployment.MultiTenantType:
		suppressUser.RegulationBackendURL += fmt.Sprintf("/dataplane/namespaces/%s/regulations/suppressions", suppressUser.ID.ID())
	default:
		panic("invalid deployment type")
	}
	rruntime.Go(func() {
		suppressUser.regulationSyncLoop(ctx)
	})
}

func (suppressUser *SuppressRegulationHandler) IsSuppressedUser(workspaceID, userID, sourceID string) bool {
	suppressUser.init()
	pkgLogger.Debugf("IsSuppressedUser called for %v, %v, %v", workspaceID, sourceID, userID)
	suppressUser.regulationsSubscriberLock.RLock()
	defer suppressUser.regulationsSubscriberLock.RUnlock()
	if _, ok := suppressUser.userSpecificSuppressedSourceMap[workspaceID]; ok {
		if _, ok := suppressUser.userSpecificSuppressedSourceMap[workspaceID][userID]; ok {
			m := suppressUser.userSpecificSuppressedSourceMap[workspaceID][userID]
			if m.all {
				return true
			}
			if _, ok := m.specific[sourceID]; ok {
				return true
			}
		}
	}
	return false
}

// Gets the regulations from data regulation service
func (suppressUser *SuppressRegulationHandler) regulationSyncLoop(ctx context.Context) {
	suppressUser.init()
	pageSize, err := strconv.Atoi(suppressUser.pageSize)
	if err != nil {
		pkgLogger.Error("invalid page size")
		suppressUser.pageSize = ""
		pageSize = 0
	}

	for {
		if ctx.Err() != nil {
			return
		}
		pkgLogger.Info("Fetching Regulations")
		regulations, err := suppressUser.getSourceRegulationsFromRegulationService()
		if err != nil {
			misc.SleepCtx(ctx, regulationsPollInterval)
			continue
		}
		// need to discuss the correct place tp put this lock
		suppressUser.regulationsSubscriberLock.Lock()
		for _, sourceRegulation := range regulations {
			userId := sourceRegulation.UserID
			workspaceID := sourceRegulation.WorkspaceID
			_, ok := suppressUser.userSpecificSuppressedSourceMap[workspaceID]
			if !ok {
				suppressUser.userSpecificSuppressedSourceMap[workspaceID] = make(map[string]sourceFilter)
			}
			if len(sourceRegulation.SourceIDs) == 0 {
				if _, ok := suppressUser.userSpecificSuppressedSourceMap[workspaceID][userId]; !ok {
					if !sourceRegulation.Canceled {
						m := sourceFilter{
							all:      true,
							specific: map[string]struct{}{},
						}
						suppressUser.userSpecificSuppressedSourceMap[workspaceID][userId] = m
						continue
					}
				}
				m := suppressUser.userSpecificSuppressedSourceMap[workspaceID][userId]
				if sourceRegulation.Canceled {
					m.all = false
				} else {
					m.all = true
				}
				suppressUser.userSpecificSuppressedSourceMap[workspaceID][userId] = m
			} else {
				if _, ok := suppressUser.userSpecificSuppressedSourceMap[workspaceID][userId]; !ok {
					if !sourceRegulation.Canceled {
						m := sourceFilter{
							specific: map[string]struct{}{},
						}
						for _, srcId := range sourceRegulation.SourceIDs {
							m.specific[srcId] = struct{}{}
						}
						suppressUser.userSpecificSuppressedSourceMap[workspaceID][userId] = m
						continue
					}
				}
				m := suppressUser.userSpecificSuppressedSourceMap[workspaceID][userId]
				if sourceRegulation.Canceled {
					for _, srcId := range sourceRegulation.SourceIDs {
						delete(m.specific, srcId) // will be no-op if key is not there in map
					}
				} else {
					for _, srcId := range sourceRegulation.SourceIDs {
						m.specific[srcId] = struct{}{}
					}
				}
				suppressUser.userSpecificSuppressedSourceMap[workspaceID][userId] = m
			}
		}
		suppressUser.regulationsSubscriberLock.Unlock()

		if len(regulations) == 0 || len(regulations) < pageSize {
			misc.SleepCtx(ctx, regulationsPollInterval)
		}
	}
}

func (suppressUser *SuppressRegulationHandler) getSourceRegulationsFromRegulationService() ([]sourceRegulation, error) {
	urlStr := suppressUser.RegulationBackendURL
	urlValQuery := url.Values{}
	if suppressUser.pageSize != "" {
		urlValQuery.Set("pageSize", suppressUser.pageSize)
	}
	if suppressUser.suppressAPIToken != "" {
		urlValQuery.Set("pageToken", suppressUser.suppressAPIToken)
	}
	if len(urlValQuery) > 0 {
		urlStr += "?" + urlValQuery.Encode()
	}

	var resp *http.Response
	var respBody []byte

	operation := func() error {
		var err error
		req, err := http.NewRequest("GET", urlStr, http.NoBody)
		pkgLogger.Debugf("regulation service URL: %s", urlStr)
		if err != nil {
			return err
		}
		req.SetBasicAuth(suppressUser.ID.BasicAuth())
		req.Header.Set("Content-Type", "application/json")

		resp, err = suppressUser.Client.Do(req)
		if err != nil {
			return err
		}
		// If statusCode is not 2xx, then returning empty regulations
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			err = fmt.Errorf("status code %v", resp.StatusCode)
			pkgLogger.Errorf("[[ Workspace-config ]] Failed to fetch source regulations. statusCode: %v, error: %v",
				resp.StatusCode, err)
			return err
		}

		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				pkgLogger.Error(err)
			}
		}(resp.Body)

		respBody, err = io.ReadAll(resp.Body)
		if err != nil {
			pkgLogger.Error(err)
			return err
		}
		return err
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		pkgLogger.Errorf("[[ Workspace-config ]] Failed to fetch source regulations from API with error: %v, retrying after %v", err, t)
	})
	if err != nil {
		pkgLogger.Error("Error sending request to the server: ", err)
		return []sourceRegulation{}, err
	}
	if respBody == nil {
		pkgLogger.Error("nil response body, returning")
		return []sourceRegulation{}, errors.New("nil response body")
	}
	var sourceRegulationsJSON apiResponse
	err = json.Unmarshal(respBody, &sourceRegulationsJSON)
	if err != nil {
		pkgLogger.Error("Error while parsing request: ", err, resp.StatusCode)
		return []sourceRegulation{}, err
	}
	// TODO: remove this once regulation Service is updated
	for i := range sourceRegulationsJSON.SourceRegulations {
		sourceRegulation := &sourceRegulationsJSON.SourceRegulations[i]
		if sourceRegulation.WorkspaceID == "" {
			sourceRegulation.WorkspaceID = suppressUser.ID.ID()
		}
	}

	if sourceRegulationsJSON.Token == "" {
		pkgLogger.Errorf("[[ Workspace-config ]] No token found in the source regulations response: %v", string(respBody))
		return sourceRegulationsJSON.SourceRegulations, fmt.Errorf("no token returned in regulation API response")
	}
	suppressUser.suppressAPIToken = sourceRegulationsJSON.Token
	return sourceRegulationsJSON.SourceRegulations, nil
}

func (suppressUser *SuppressRegulationHandler) init() {
	suppressUser.once.Do(func() {
		pkgLogger.Info("init Regulations")
		if len(suppressUser.userSpecificSuppressedSourceMap) == 0 {
			suppressUser.userSpecificSuppressedSourceMap = map[string]map[string]sourceFilter{}
		}
		if suppressUser.Client == nil {
			suppressUser.Client = &http.Client{Timeout: config.GetDuration("HttpClient.suppressUser.timeout", 30, time.Second)}
		}
	})
}

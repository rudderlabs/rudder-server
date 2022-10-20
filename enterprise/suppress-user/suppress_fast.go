package suppression

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	jsonfast "github.com/goccy/go-json"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/rudderlabs/rudder-server/rruntime"
)

// SuppressFast is a handle to this object
type SuppressFast struct {
	Client                          *http.Client
	RegulationBackendURL            string
	RegulationsPollInterval         time.Duration
	WorkspaceID                     string
	userSpecificSuppressedSourceMap map[string]sourceFilter
	regulationsSubscriberLock       sync.RWMutex
	suppressAPIToken                string
	PageSize                        string
	once                            sync.Once
}

func (suppressUser *SuppressFast) setup(ctx context.Context) {
	rruntime.Go(func() {
		suppressUser.regulationSyncLoop(ctx)
	})
}

func (suppressUser *SuppressFast) Run(ctx context.Context) {
	suppressUser.regulationSyncLoop(ctx)

	fmt.Println(len(suppressUser.userSpecificSuppressedSourceMap))
}

func (suppressUser *SuppressFast) IsSuppressedUser(userID, sourceID string) bool {
	suppressUser.init()
	pkgLogger.Debugf("IsSuppressedUser called for %v, %v", sourceID, userID)
	suppressUser.regulationsSubscriberLock.RLock()
	defer suppressUser.regulationsSubscriberLock.RUnlock()
	if _, ok := suppressUser.userSpecificSuppressedSourceMap[userID]; ok {
		m := suppressUser.userSpecificSuppressedSourceMap[userID]
		if m.all {
			return true
		}
		if _, ok := m.specific[sourceID]; ok {
			return true
		}
	}
	return false
}

type RegulationIter struct {
	Decoder *jsonfast.Decoder
	f       io.ReadCloser
	err     error
	reg     sourceRegulation
	count   int
	inItems bool
}

func (iter *RegulationIter) Close() {

	if iter.Decoder == nil {
		// read the end of the array
		_, err := io.ReadAll(iter.Decoder.Buffered())
		if err != nil {
			iter.err = err
		}
		iter.Decoder = nil
	}
	iter.f.Close()
}

func (iter *RegulationIter) findItems() bool {
	t, err := iter.Decoder.Token()
	if err != nil {
		iter.err = err
		return false
	}

	if t != json.Delim('{') {
		iter.err = fmt.Errorf("expected {, got %v", t)
		return false
	}

	for iter.Decoder.More() {
		t, err = iter.Decoder.Token()
		if err != nil {
			iter.err = err
			return false
		}
		k, ok := t.(string)
		if !ok {
			continue
		}
		if k == "items" {
			t, err := iter.Decoder.Token()
			if err != nil {
				iter.err = err
				return false
			}

			if t != json.Delim('[') {
				iter.err = fmt.Errorf("expected {, got %v", t)
				return false
			}

			iter.inItems = true
			return true
		}
	}

	iter.err = fmt.Errorf("items not found")
	return false
}

func (iter *RegulationIter) Next() bool {

	if !iter.inItems {
		if !iter.findItems() {
			return false
		}
	}

	if iter.Decoder.More() {
		err := iter.Decoder.Decode(&iter.reg)
		if err != nil {
			iter.err = err
			return false
		}
		iter.count++
		return true
	} else {
		iter.Close()
		return false
	}
}

func (iter *RegulationIter) Item() *sourceRegulation {
	return &iter.reg
}

func (iter *RegulationIter) Count() int {
	return iter.count
}

func (iter *RegulationIter) Error() error {
	return iter.err
}

// Gets the regulations from data regulation service
func (suppressUser *SuppressFast) regulationSyncLoop(ctx context.Context) {
	suppressUser.init()
	pageSize, err := strconv.Atoi(suppressUser.PageSize)
	if err != nil {
		pkgLogger.Error("invalid page size")
		suppressUser.PageSize = ""
		pageSize = 0
	}

	for {
		if ctx.Err() != nil {
			return
		}
		iter, err := suppressUser.getSourceRegulationsFromRegulationService()
		if err != nil {
			pkgLogger.Errorf("Error getting regulations from regulation service: %v", err)
			misc.SleepCtx(ctx, regulationsPollInterval)
			continue
		}

		// need to discuss the correct place tp put this lock
		suppressUser.regulationsSubscriberLock.Lock()

		for iter.Next() {
			sourceRegulation := iter.Item()
			userId := sourceRegulation.UserID
			if len(sourceRegulation.SourceIDs) == 0 {
				if _, ok := suppressUser.userSpecificSuppressedSourceMap[userId]; !ok {
					if !sourceRegulation.Canceled {
						m := sourceFilter{
							all:      true,
							specific: map[string]struct{}{},
						}
						suppressUser.userSpecificSuppressedSourceMap[userId] = m
						continue
					}
				}
				m := suppressUser.userSpecificSuppressedSourceMap[userId]
				if sourceRegulation.Canceled {
					m.all = false
				} else {
					m.all = true
				}
				suppressUser.userSpecificSuppressedSourceMap[userId] = m
			} else {
				if _, ok := suppressUser.userSpecificSuppressedSourceMap[userId]; !ok {
					if !sourceRegulation.Canceled {
						m := sourceFilter{
							specific: map[string]struct{}{},
						}
						for _, srcId := range sourceRegulation.SourceIDs {
							m.specific[srcId] = struct{}{}
						}
						suppressUser.userSpecificSuppressedSourceMap[userId] = m
						continue
					}
				}
				m := suppressUser.userSpecificSuppressedSourceMap[userId]
				if sourceRegulation.Canceled {
					for _, srcId := range sourceRegulation.SourceIDs {
						delete(m.specific, srcId) // will be no-op if key is not there in map
					}
				} else {
					for _, srcId := range sourceRegulation.SourceIDs {
						m.specific[srcId] = struct{}{}
					}
				}
				suppressUser.userSpecificSuppressedSourceMap[userId] = m
			}
		}
		suppressUser.regulationsSubscriberLock.Unlock()
		if iter.Error() != nil {
			pkgLogger.Error(iter.Error())
			misc.SleepCtx(ctx, regulationsPollInterval)
			continue
		}

		iter.Close()

		if iter.Count() == 0 || iter.Count() < pageSize {
			misc.SleepCtx(ctx, regulationsPollInterval)
		}
	}
}

func (suppressUser *SuppressFast) getSourceRegulationsFromRegulationService() (*RegulationIter, error) {
	if config.GetBool("HOSTED_SERVICE", false) {
		pkgLogger.Info("[Regulations] Regulations on free tier are not supported at the moment.")
		return nil, nil
	}

	urlStr := fmt.Sprintf("%s/dataplane/workspaces/%s/regulations/suppressions", suppressUser.RegulationBackendURL, suppressUser.WorkspaceID)
	urlValQuery := url.Values{}

	if suppressUser.suppressAPIToken != "" {
		urlValQuery.Set("pageToken", suppressUser.suppressAPIToken)
	}
	urlValQuery.Set("pageSize", suppressUser.PageSize)

	if len(urlValQuery) > 0 {
		urlStr += "?" + urlValQuery.Encode()
	}

	var resp *http.Response

	operation := func() error {
		var err error
		req, err := http.NewRequest("GET", urlStr, http.NoBody)
		pkgLogger.Debugf("regulation service URL: %s", urlStr)
		if err != nil {
			return err
		}
		workspaceToken := config.GetWorkspaceToken()
		req.SetBasicAuth(workspaceToken, "")
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

		return err
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		pkgLogger.Errorf("[[ Workspace-config ]] Failed to fetch source regulations from API with error: %v, retrying after %v", err, t)
	})
	if err != nil {
		pkgLogger.Error("Error sending request to the server: ", err)
		return nil, err
	}

	suppressUser.suppressAPIToken = resp.Header.Get("X-Page-Token")

	return &RegulationIter{
		Decoder: jsonfast.NewDecoder(resp.Body),
		f:       resp.Body,
	}, nil
}

func (suppressUser *SuppressFast) init() {
	suppressUser.once.Do(func() {
		pkgLogger.Info("init Regulations")
		if len(suppressUser.userSpecificSuppressedSourceMap) == 0 {
			suppressUser.userSpecificSuppressedSourceMap = map[string]sourceFilter{}
		}
		if suppressUser.Client == nil {
			suppressUser.Client = &http.Client{Timeout: config.GetDuration("HttpClient.suppressUser.timeout", 30, time.Second)}
		}
	})
}

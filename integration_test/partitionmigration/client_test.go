package partitionmigration_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/partmap"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

type gatewayClientConfig struct {
	url                       string
	writeKey                  string
	numPartitions             int
	jobsPerPartitionPerSecond int
}

func startGatewayClient(ctx context.Context, g *errgroup.Group, cfg gatewayClientConfig, l logger) *gatewayClient {
	ctx, cancel := context.WithCancel(ctx)
	clientG, ctx := errgroup.WithContext(ctx)

	gc := &gatewayClient{
		clientG: clientG,
		cancel:  cancel,
		stopped: make(chan struct{}),
	}
	for i := 0; i < cfg.numPartitions; i++ {
		userID := gc.getUserIDForPartition(i, cfg.numPartitions)
		l.Logf("Will be using user id %q for partition %d", userID, i)
		clientG.Go(func() error {
			var orderIdx int
			sleepFor := time.Second
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(sleepFor):
					start := time.Now()
					jobs := gc.generateJobs(userID, orderIdx, cfg.jobsPerPartitionPerSecond)
					orderIdx += cfg.jobsPerPartitionPerSecond
					for _, job := range jobs {
						err := gc.sendRequest(cfg.url, cfg.writeKey, userID, job)
						if err != nil && ctx.Err() == nil {
							return fmt.Errorf("sending request for user %s: %w", userID, err)
						}
					}
					elapsed := time.Since(start)
					if elapsed < sleepFor {
						sleepFor = sleepFor - elapsed
					} else {
						sleepFor = 0
					}
				}
			}
		})
	}

	g.Go(func() error {
		gc.err = clientG.Wait()
		close(gc.stopped)
		return gc.err
	})

	return gc
}

type gatewayClient struct {
	clientG   *errgroup.Group
	cancel    context.CancelFunc
	stopped   chan struct{}
	err       error
	totalSent atomic.Int64
	stopOnce  sync.Once
}

func (gc *gatewayClient) Stop() error {
	gc.stopOnce.Do(func() {
		gc.cancel()
		<-gc.stopped
	})
	return gc.err
}

func (gc *gatewayClient) GetTotalSent() int64 {
	return gc.totalSent.Load()
}

func (gc *gatewayClient) generateJobs(userID string, startIndex, jobCount int) (jobs []string) {
	for j := startIndex; j < startIndex+jobCount; j++ {
		messageID := uuid.New().String()
		timestamp := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
		jobs = append(jobs, `{
		"userId": "`+userID+`",
		"messageId":"`+messageID+`",
		"type": "identify",
		"eventIndex": `+strconv.Itoa(j)+`,
		"context": {
		},
		"timestamp": "`+timestamp+`"
	}`)
	}
	return jobs
}

func (gc *gatewayClient) getUserIDForPartition(partitionIdx, numPartitions int) string {
	for i := range 1000000 {
		candidate := "user" + strconv.Itoa(i)
		idx, _ := partmap.Murmur3Partition32(legacyUserID(candidate), uint32(numPartitions))
		if idx == uint32(partitionIdx) {
			return candidate
		}
	}
	return ""
}

// gateway component doesn't hash just the userID, but a weird combination of separators and ids of some sort
func legacyUserID(userID string) string {
	const separator = "<<>>"
	return separator + userID + separator + userID
}

func (gc *gatewayClient) sendRequest(urlString, writeKey, userID, payload string) error {
	u, _ := url.Parse(urlString)
	u.Path = path.Join(u.Path, "v1", "identify")
	requestURL := u.String()
	req, err := http.NewRequest(http.MethodPost, requestURL, strings.NewReader(payload))
	if err != nil {
		return fmt.Errorf("creating http request: %w", err)
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(writeKey+":")))
	req.Header.Add("X-Partition-Key", legacyUserID(userID))
	res, err := http.DefaultClient.Do(req)
	defer func() { httputil.CloseResponse(res) }()
	if err != nil {
		return fmt.Errorf("doing http request: %w", err)
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", res.StatusCode)
	}
	gc.totalSent.Add(1)
	return nil
}

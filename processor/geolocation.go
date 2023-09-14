package processor

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/processor/geolocation"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type PipelineEnricher interface {
	Enrich(sourceId string, request *types.GatewayBatchRequest)
}

type FileManager interface {
	Download(context.Context, *os.File, string) error
}

type geolocationEnricher struct {
	fetcher     geolocation.GeoFetcher
	logger      logger.Logger
	fileManager FileManager
	stats       stats.Stats
}

func NewGeoEnricher(config *config.Config, log logger.Logger, stat stats.Stats) (PipelineEnricher, error) {

	var (
		bucket       = config.GetString("Geolocation.db.bucket", "rudderstack-geolocation")
		region       = config.GetString("Geolocation.db.bucket.region", "us-east-1")
		key          = config.GetString("Geolocation.db.key.path", "geolite2City.mmdb")
		downloadPath = config.GetString("Geolocation.db.downloadPath", "geolite2City.mmdb")
	)

	s3Manager, err := filemanager.NewS3Manager(map[string]interface{}{
		"Bucket": bucket,
		"Region": region,
	}, log, func() time.Duration { return 1000 * time.Millisecond })
	if err != nil {
		return nil, fmt.Errorf("creating a new instance of s3 file manager: %w", err)
	}

	f, err := os.Create(downloadPath)
	if err != nil {
		return nil, fmt.Errorf("creating local file for storing database: %w", err)
	}

	defer f.Close()

	err = s3Manager.Download(context.Background(), f, key)
	if err != nil {
		return nil, fmt.Errorf("downloading instance of database from upstream: %w", err)
	}

	fetcher, err := geolocation.NewMaxmindGeoFetcher(downloadPath)
	if err != nil {
		return nil, fmt.Errorf("creating new instance of maxmind's geolocation fetcher: %w", err)
	}

	return &geolocationEnricher{
		fetcher:     fetcher,
		fileManager: s3Manager,
		stats:       stat,
		logger:      log.Child("geolocation"),
	}, nil
}

// Enrich function runs on a request of GatewayBatchRequest which contains
// multiple singular events from a source. The enrich function augments the
// geolocation information per event based on IP address.
func (e *geolocationEnricher) Enrich(sourceId string, request *types.GatewayBatchRequest) {
	e.logger.Debugw("received a call to enrich gateway events for the customer", "sourceId", sourceId)

	if request.RequestIP == "" {
		e.stats.NewTaggedStat("proc_geo_enrincher_empty_ip", stats.CountType, stats.Tags{
			"sourceId": sourceId,
		}).Increment()
		return
	}

	defer func(from time.Time) {
		e.stats.NewTaggedStat(
			"pro_geo_enricher_request_latency",
			stats.TimerType,
			stats.Tags{"sourceId": sourceId}).Since(from)
	}(time.Now())

	parsedIP := net.ParseIP(request.RequestIP)
	if parsedIP == nil {
		e.stats.NewTaggedStat("proc_geo_enricher_invalid_ip", stats.CountType, stats.Tags{
			"sourceId": sourceId,
		}).Increment()
		return
	}

	geoip, err := e.fetcher.GeoIP(parsedIP)
	if err != nil {
		e.logger.Errorw("unable to enrich the request with geolocation", "error", err)
		e.stats.NewTaggedStat(
			"proc_geo_enricher_geoip_lookup_failed",
			stats.CountType,
			stats.Tags{"sourceId": sourceId}).Increment()
		return
	}

	for _, event := range request.Batch {
		if context, ok := event["context"].(map[string]interface{}); ok {
			context["geo"] = geoip
		}
	}
}

type NoOpGeoEnricher struct {
}

func (NoOpGeoEnricher) Enrich(sourceId string, request *types.GatewayBatchRequest) {
}

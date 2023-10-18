package enricher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/geolocation"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type GeoDBFetcher interface {
	GetDB(ctx context.Context, key, downloadPath string) error
}

type Geolocation struct {
	City     string `json:"city"`
	Country  string `json:"country"`
	Region   string `json:"region"`
	Postal   string `json:"postal"`
	Location string `json:"location"`
	Timezone string `json:"timezone"`
}

type geoEnricher struct {
	fetcher geolocation.GeoFetcher
	logger  logger.Logger
	stats   stats.Stats
}

func NewGeoEnricher(
	dbProvider GeoDBFetcher,
	conf *config.Config,
	log logger.Logger,
	statClient stats.Stats,
) (PipelineEnricher, error) {
	upstreamDBKey := conf.GetString("Geolocation.db.key", "geolite2City.mmdb")
	downloadPath := conf.GetString("Geolocation.db.downloadPath", "geolite2City.mmdb")

	err := dbProvider.GetDB(context.Background(), upstreamDBKey, downloadPath)
	if err != nil {
		return nil, fmt.Errorf("getting db from upstream: %w", err)
	}

	fetcher, err := geolocation.NewMaxmindDBReader(downloadPath)
	if err != nil {
		return nil, fmt.Errorf("creating new instance of maxmind's geolocation fetcher: %w", err)
	}

	return &geoEnricher{
		fetcher: fetcher,
		stats:   statClient,
		logger:  log.Child("geolocation"),
	}, nil
}

// Enrich function runs on a request of GatewayBatchRequest which contains
// multiple singular events from a source. The enrich function augments the
// geolocation information per event based on IP address.
func (e *geoEnricher) Enrich(sourceID string, request *types.GatewayBatchRequest) error {
	e.logger.Debugw("received a call to enrich gateway events for source", "sourceId", sourceID)

	if request.RequestIP == "" {
		e.stats.NewTaggedStat("proc_geo_enrincher_empty_ip", stats.CountType, stats.Tags{
			"sourceId": sourceID,
		}).Increment()

		return nil
	}

	defer e.stats.NewTaggedStat(
		"proc_geo_enricher_request_latency",
		stats.TimerType,
		stats.Tags{"sourceId": sourceID}).RecordDuration()()

	geoip, err := e.fetcher.Locate(request.RequestIP)
	if err != nil {
		e.stats.NewTaggedStat(
			"proc_geo_enricher_geoip_lookup_failed",
			stats.CountType,
			stats.Tags{
				"sourceId": sourceID,
				"validIP":  strconv.FormatBool(errors.Is(err, geolocation.ErrInvalidIP)),
			}).Increment()

		return fmt.Errorf("enriching request with geolocation: %w", err)
	}

	// for every event if we have context object set
	// only then we add to geo section
	geoData := extractGeolocationData(geoip)

	for _, event := range request.Batch {
		if context, ok := event["context"].(map[string]interface{}); ok {
			context["geo"] = geoData
		}
	}

	return nil
}

type NoOpGeoEnricher struct{}

func (e NoOpGeoEnricher) Enrich(string, *types.GatewayBatchRequest) error {
	return nil
}

type geoDB struct {
	s3Manager filemanager.FileManager
}

// GetDB simply fetches the database from the upstream located at the key defined
// in the argument and stores it in the downloadPath provided.
func (db *geoDB) GetDB(ctx context.Context, key, downloadPath string) error {
	// If the file already exists, do not go into the loop of downloading
	// the file again from s3.
	if _, err := os.Stat(downloadPath); err == nil {
		return nil
	}

	f, err := os.Create(downloadPath)
	if err != nil {
		return fmt.Errorf("creating a file to store db contents: %w", err)
	}

	defer f.Close()

	err = db.s3Manager.Download(ctx, f, key)
	if err != nil {
		return fmt.Errorf("downloading db from upstream and storing in file: %w", err)
	}

	return nil
}

func NewGeoDBFetcher(conf *config.Config, log logger.Logger) (GeoDBFetcher, error) {
	var (
		bucket = conf.GetString("Geolocation.db.bucket", "rudderstack-geolocation")
		region = conf.GetString("Geolocation.db.bucket.region", "us-east-1")
	)

	manager, err := filemanager.NewS3Manager(map[string]interface{}{
		"Bucket": bucket,
		"Region": region,
	}, log, func() time.Duration {
		return 1000 * time.Millisecond
	})
	if err != nil {
		return nil, fmt.Errorf("creating a new s3 manager client: %w", err)
	}

	return &geoDB{
		s3Manager: manager,
	}, nil
}

func extractGeolocationData(geoCity *geolocation.GeoInfo) *Geolocation {
	if geoCity == nil {
		return nil
	}

	toReturn := &Geolocation{
		City:     geoCity.City.Names["en"],
		Country:  geoCity.Country.ISOCode,
		Postal:   geoCity.Postal.Code,
		Timezone: geoCity.Location.Timezone,
	}

	if len(geoCity.Subdivisions) > 0 {
		toReturn.Region = geoCity.Subdivisions[0].Names["en"]
	}

	// default values of latitude and longitude can give
	// incorrect result, so we have casted them in pointers so we know
	// when the value is missing.
	if geoCity.Location.Latitude != nil && geoCity.Location.Longitude != nil {
		toReturn.Location = fmt.Sprintf("%f,%f",
			*geoCity.Location.Latitude,
			*geoCity.Location.Longitude,
		)
	}

	return toReturn
}

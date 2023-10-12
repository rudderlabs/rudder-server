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

type GeoDBProvider interface {
	GetDB(string, string) error
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
	dbProvider GeoDBProvider,
	config *config.Config,
	log logger.Logger,
	statClient stats.Stats,
) (PipelineEnricher, error) {
	var upstreamDBKey = config.GetString("Geolocation.db.key", "geolite2City.mmdb")
	var downloadPath = config.GetString("Geolocation.db.downloadPath", "geolite2City.mmdb")

	err := dbProvider.GetDB(upstreamDBKey, downloadPath)
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
func (e *geoEnricher) Enrich(sourceId string, request *types.GatewayBatchRequest) error {
	e.logger.Debugw("received a call to enrich gateway events for source", "sourceId", sourceId)

	if request.RequestIP == "" {
		e.stats.NewTaggedStat("proc_geo_enrincher_empty_ip", stats.CountType, stats.Tags{
			"sourceId": sourceId,
		}).Increment()

		return nil
	}

	defer func(from time.Time) {
		e.stats.NewTaggedStat(
			"proc_geo_enricher_request_latency",
			stats.TimerType,
			stats.Tags{"sourceId": sourceId}).Since(from)
	}(time.Now())

	geoip, err := e.fetcher.GeoIP(request.RequestIP)
	if err != nil {
		e.stats.NewTaggedStat(
			"proc_geo_enricher_geoip_lookup_failed",
			stats.CountType,
			stats.Tags{
				"sourceId": sourceId,
				"validIP":  strconv.FormatBool(errors.Is(err, geolocation.ErrInvalidIP)),
			}).Increment()

		return fmt.Errorf("unable to enrich the request with geolocation: %w", err)
	}

	// for every event if we have context object set
	// only then we add to geo section
	for _, event := range request.Batch {
		if context, ok := event["context"].(map[string]interface{}); ok {
			context["geo"] = extractGeolocationData(geoip)
		}
	}

	return nil
}

type NoOpGeoEnricher struct {
}

func (e NoOpGeoEnricher) Enrich(sourceId string, request *types.GatewayBatchRequest) error {
	return nil
}

type geoDBProviderImpl struct {
	s3Manager filemanager.FileManager
}

// GetDB simply fetches the database from the upstream located at the key defined
// in the argument.
func (p *geoDBProviderImpl) GetDB(key string, downloadPath string) error {
	f, err := os.Open(downloadPath)
	if err != nil {
		return fmt.Errorf("creating a file to store db contents: %w", err)
	}

	defer f.Close()

	err = p.s3Manager.Download(context.Background(), f, key)
	if err != nil {
		return fmt.Errorf("downloading db from upstream and storing in file: %w", err)
	}

	return nil
}

func NewGeoDBProvider(conf *config.Config, log logger.Logger) (GeoDBProvider, error) {
	var (
		bucket = config.GetString("Geolocation.db.bucket", "rudderstack-geolocation")
		region = config.GetString("Geolocation.db.bucket.region", "us-east-1")
	)

	manager, err := filemanager.NewS3Manager(map[string]interface{}{
		"Bucket": bucket,
		"Region": region}, log, func() time.Duration {
		return 1000 * time.Millisecond
	})

	if err != nil {
		return nil, fmt.Errorf("creating a new s3 manager client: %w", err)
	}

	return &geoDBProviderImpl{
		s3Manager: manager,
	}, nil
}

func extractGeolocationData(geocity *geolocation.GeoCity) *Geolocation {
	if geocity == nil {
		return nil
	}

	toReturn := &Geolocation{
		City:     geocity.City.Names["en"],
		Country:  geocity.Country.IsoCode,
		Postal:   geocity.Postal.Code,
		Timezone: geocity.Location.TimeZone,
	}

	if len(geocity.Subdivisions) > 0 {
		toReturn.Region = geocity.Subdivisions[0].Names["en"]
	}

	// default values of latitude and longitude can give
	// incorrect result, so we have casted them in pointers so we know
	// when the value is missing.
	if geocity.Location.Latitude != nil && geocity.Location.Longitude != nil {
		toReturn.Location = fmt.Sprintf("%f,%f",
			*geocity.Location.Latitude,
			*geocity.Location.Longitude,
		)
	}

	return toReturn
}

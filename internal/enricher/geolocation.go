package enricher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/geolocation"
	"github.com/rudderlabs/rudder-server/utils/types"
)

const (
	ERR_INVALID_IP    = "invalid_ip"
	ERR_EMPTY_IP      = "empty_ip"
	ERR_LOCATE_FAILED = "locate_failed"
)

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

func NewGeoEnricher(conf *config.Config, log logger.Logger, statClient stats.Stats) (PipelineEnricher, error) {
	log.Infof("Setting up new geo enricher")

	dbPath, err := downloadMaxmindDB(context.Background(), conf, log)
	if err != nil {
		return nil, fmt.Errorf("downloading instance of maxmind db: %w", err)
	}

	fetcher, err := geolocation.NewMaxmindDBReader(dbPath)
	if err != nil {
		return nil, fmt.Errorf("creating new instance of maxmind's geolocation db reader: %w", err)
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
func (e *geoEnricher) Enrich(source *backendconfig.SourceT, request *types.GatewayBatchRequest) error {
	if !source.GeoEnrichment.Enabled {
		return nil
	}
	e.logger.Debugw("received a call to enrich gateway events for source", "sourceID", source.ID)

	var (
		rawGeo *geolocation.GeoInfo
		err    error
	)

	defer func() {
		errType := ""

		if request.RequestIP == "" {
			errType = ERR_EMPTY_IP
		}

		if err != nil {
			errType = ERR_LOCATE_FAILED

			if errors.Is(err, geolocation.ErrInvalidIP) {
				errType = ERR_INVALID_IP
			}
		}

		e.stats.NewTaggedStat(
			"proc_geo_enricher_request",
			stats.CountType,
			stats.Tags{
				"sourceID": source.ID,
				"error":    errType,
			}).Increment()
	}()

	if request.RequestIP == "" {
		return nil
	}

	defer e.stats.NewTaggedStat(
		"proc_geo_enricher_request_latency",
		stats.TimerType,
		stats.Tags{
			"sourceID": source.ID,
		},
	).RecordDuration()()

	rawGeo, err = e.fetcher.Locate(request.RequestIP)
	if err != nil {
		return fmt.Errorf("enriching request with geolocation: %w", err)
	}

	geoData := extractGeolocationData(rawGeo)

	for _, event := range request.Batch {
		// if the context section is missing on the event
		// set it with default as map[string]interface{}
		if _, ok := event["context"]; !ok {
			event["context"] = map[string]interface{}{}
		}

		if context, ok := event["context"].(map[string]interface{}); ok {
			// if the `geo` key already present fire a stat for observability
			if _, ok := context["geo"]; ok {
				continue
			}
			// update the geo section
			context["geo"] = geoData
		}
	}

	return nil
}

func (e *geoEnricher) Close() error {
	if err := e.fetcher.Close(); err != nil {
		return fmt.Errorf("closing the geo enricher: %w", err)
	}
	return nil
}

func downloadMaxmindDB(ctx context.Context, conf *config.Config, log logger.Logger) (string, error) {
	var (
		dbKey  = conf.GetString("Geolocation.db.key", "geolite2City.mmdb")
		bucket = conf.GetString("Geolocation.db.bucket", "rudderstack-geolocation")
		region = conf.GetString("Geolocation.db.bucket.region", "us-east-1")
	)

	log.Infof("downloading new geolocation db from key: %s", dbKey)
	var (
		tmpDirPath = strings.TrimSuffix(conf.GetString("RUDDER_TMPDIR", "."), "/")
		baseDIR    = fmt.Sprintf("%s/geolocation", tmpDirPath)
		fullpath   = fmt.Sprintf("%s/%s", baseDIR, dbKey)
	)

	// If the filepath exists return
	_, err := os.Stat(fullpath)
	if err == nil {
		return fullpath, nil
	}

	err = os.MkdirAll(baseDIR, os.ModePerm)
	if err != nil {
		return "", fmt.Errorf("creating directory for storing db: %w", err)
	}

	f, err := os.CreateTemp(baseDIR, "geodb-*.mmdb")
	if err != nil {
		return "", fmt.Errorf("creating a temporary file: %w", err)
	}

	// Make sure we close and remove the file
	defer func() {
		f.Close()
		os.Remove(fmt.Sprintf("%s/%s", baseDIR, f.Name()))
	}()

	manager, err := filemanager.NewS3Manager(map[string]interface{}{
		"bucketName": bucket,
		"region":     region,
	}, log, func() time.Duration {
		return 100 * time.Second
	})
	if err != nil {
		return "", fmt.Errorf("creating a new s3 manager client: %w", err)
	}

	err = manager.Download(ctx, f, dbKey)
	if err != nil {
		return "", fmt.Errorf("downloading file with key: %s from bucket: %s and region: %s, err: %w",
			dbKey,
			bucket,
			region,
			err)
	}

	// Finally move the downloaded file from previous temp location to new location
	err = os.Rename(f.Name(), fullpath)
	if err != nil {
		return "", fmt.Errorf("renaming file: %w", err)
	}

	return fullpath, nil
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

package enricher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/samber/lo"

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
	IP       string `json:"ip"`
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
	log.Infof("Setting up new event geo enricher")

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
	defer e.stats.NewTaggedStat(
		"proc_geo_enricher_request_latency",
		stats.TimerType,
		stats.Tags{
			"sourceId":    source.ID,
			"sourceType":  source.SourceDefinition.Type,
			"workspaceId": source.WorkspaceID,
		},
	).RecordDuration()()

	var enrichErrs []error
	for _, event := range request.Batch {
		// if the context section is missing on the event
		// set it with default as map[string]interface{}
		if _, ok := event["context"]; !ok {
			event["context"] = map[string]interface{}{}
		}

		// if the context is other than map[string]interface{}, add error and continue
		context, ok := event["context"].(map[string]interface{})
		if !ok {
			enrichErrs = append(enrichErrs, fmt.Errorf("event on source: %s doesn't have a valid context section", source.ID))
			continue
		}

		// if the `geo` key already present on the event, continue
		if _, ok := context["geo"]; ok {
			continue
		}

		contextIP, _ := context["ip"].(string)
		ip, _ := lo.Find([]string{contextIP, request.RequestIP}, func(v string) bool { return v != "" })

		errType := ""
		rawGeo, err := e.fetcher.Locate(ip)
		if err != nil {
			if errors.Is(err, geolocation.ErrInvalidIP) {
				// `emptyIP` even though it's invalid is treated differently
				// to get better stats about how many non-empty values are coming in which are invalid.
				if ip == "" {
					errType = ERR_EMPTY_IP
				} else {
					errType = ERR_INVALID_IP
				}
			} else {
				// empty / invalidIP's are not terminal errors but
				// any error except that mean the database failed for lookup
				errType = ERR_LOCATE_FAILED
				enrichErrs = append(enrichErrs, fmt.Errorf("locating geolocation for ip: %w", err))
			}
		}

		e.stats.NewTaggedStat(
			"proc_geo_enricher_request",
			stats.CountType,
			stats.Tags{
				"sourceId":    source.ID,
				"workspaceId": source.WorkspaceID,
				"sourceType":  source.SourceDefinition.Type,
				"error":       errType,
			}).Increment()

		// Set the empty data on the context nonetheless
		context["geo"] = extractGeolocationData(ip, rawGeo)
	}

	return errors.Join(enrichErrs...)
}

func (e *geoEnricher) Close() error {
	e.logger.Info("closing the geolocation enricher")

	if err := e.fetcher.Close(); err != nil {
		return fmt.Errorf("closing the geo enricher: %w", err)
	}
	return nil
}

// downloadMaxmindDB downloads database file from upstream s3 and stores it in
// a specified location. Download is skipped if the file already exists in the expected path.
func downloadMaxmindDB(ctx context.Context, conf *config.Config, log logger.Logger) (string, error) {
	var (
		dbKey            = conf.GetString("Geolocation.db.key", "geolite2City.mmdb")
		bucket           = conf.GetString("Geolocation.db.storage.bucket", "rudderstack-geolocation")
		region           = conf.GetString("Geolocation.db.storage.region", "us-east-1")
		endpoint         = conf.GetString("Geolocation.db.storage.endpoint", "")
		accessKeyID      = conf.GetString("Geolocation.db.storage.accessKey", "")
		secretAccessKey  = conf.GetString("Geolocation.db.storage.secretAccessKey", "")
		s3ForcePathStyle = conf.GetBool("Geolocation.db.storage.s3ForcePathStyle", false)
		disableSSL       = conf.GetBool("Geolocation.db.storage.disableSSL", false)
	)

	var (
		baseDIR      = path.Join(conf.GetString("RUDDER_TMPDIR", "."), "geolocation")
		downloadPath = path.Join(baseDIR, dbKey)
	)

	// If the filepath exists return
	if _, err := os.Stat(downloadPath); err == nil {
		return downloadPath, nil
	}

	log.Infof("downloading new geolocation db from key: %s", dbKey)

	if err := os.MkdirAll(baseDIR, os.ModePerm); err != nil {
		return "", fmt.Errorf("creating directory for storing db: %w", err)
	}

	f, err := os.CreateTemp(baseDIR, "geodb-*.mmdb")
	if err != nil {
		return "", fmt.Errorf("creating a temporary file: %w", err)
	}

	defer func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}()

	manager, err := filemanager.New(&filemanager.Settings{
		Provider: "S3",
		Config: map[string]interface{}{
			"bucketName":       bucket,
			"region":           region,
			"endpoint":         endpoint,
			"accessKeyID":      accessKeyID,
			"secretAccessKey":  secretAccessKey,
			"s3ForcePathStyle": s3ForcePathStyle,
			"disableSSL":       disableSSL,
		},
		Conf: conf,
	})
	if err != nil {
		return "", fmt.Errorf("creating a new s3 manager client: %w", err)
	}

	if err := manager.Download(ctx, f, dbKey); err != nil {
		return "", fmt.Errorf("downloading file with key: %s from bucket: %s and region: %s, err: %w",
			dbKey,
			bucket,
			region,
			err)
	}

	// before renaming, we need to sync data to the disk
	if err := f.Sync(); err != nil {
		return "", fmt.Errorf("syncing file to disk: %w", err)
	}

	// Finally move the downloaded file from previous temp location to new location
	if err := os.Rename(f.Name(), downloadPath); err != nil {
		return "", fmt.Errorf("renaming file: %w", err)
	}

	return downloadPath, nil
}

func extractGeolocationData(ip string, geoCity geolocation.GeoInfo) Geolocation {
	toReturn := Geolocation{
		IP:       ip,
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

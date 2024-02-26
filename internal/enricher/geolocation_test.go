package enricher

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	svcMetric "github.com/rudderlabs/rudder-go-kit/stats/metric"
	miniodocker "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/geolocation"
	"github.com/rudderlabs/rudder-server/utils/types"
)

func TestGeolocationEnrichment_Setup(t *testing.T) {
	var (
		defaultLog   = logger.NewLogger()
		defaultStats = stats.Default
	)

	t.Run("corrupted db file causes enricher to fail setup", func(t *testing.T) {
		t.Parallel()

		c := config.New()

		c.Set("RUDDER_TMPDIR", "./testdata")
		c.Set("Geolocation.db.key", "corrupted_city_test.mmdb")

		_, err := NewGeoEnricher(c, defaultLog, defaultStats)
		require.NotNil(t, err)
		require.ErrorIs(t, err, geolocation.ErrInvalidDatabase)
	})

	t.Run("correctly downloaded file causes enricher to setup correctly", func(t *testing.T) {
		t.Parallel()

		c := config.New()
		c.Set("RUDDER_TMPDIR", "./testdata")
		c.Set("Geolocation.db.key", "city_test.mmdb")

		_, err := NewGeoEnricher(c, defaultLog, defaultStats)
		require.Nil(t, err)
	})
}

func TestGeolocationEnrichment_Success(t *testing.T) {
	c := config.New()

	c.Set("RUDDER_TMPDIR", "./testdata")
	c.Set("Geolocation.db.key", "city_test.mmdb")

	enricher, err := NewGeoEnricher(c, logger.NewLogger(), stats.NewStats(
		c, logger.Default, svcMetric.Instance,
	))
	require.Nil(t, err)

	defer func() {
		require.Nil(t, enricher.Close())
	}()

	t.Run("geolocation enrichment", func(t *testing.T) {
		t.Run("it performs empty enrichment if ip is empty or invalid", func(t *testing.T) {
			t.Parallel()

			ip := &types.GatewayBatchRequest{
				RequestIP: ``,
				Batch: []types.SingularEventT{
					{"userId": "1", "context": map[string]interface{}{"app_version": "0.1.0"}},
					{"userId": "2", "context": map[string]interface{}{"app_version": "0.1.0"}},
				},
			}
			// Empty enrichment happens if the requestIP is empty / invalid
			err := enricher.Enrich(
				NewSourceBuilder("source-id").
					WithGeoEnrichment(true).
					Build(), ip)
			require.Nil(t, err)
			// require.Equal(t, nil, ip)
			require.Equal(t, types.SingularEventT{"userId": "1", "context": map[string]interface{}{"app_version": "0.1.0", "geo": Geolocation{}}}, ip.Batch[0])
			require.Equal(t, types.SingularEventT{"userId": "2", "context": map[string]interface{}{"app_version": "0.1.0", "geo": Geolocation{}}}, ip.Batch[1])
		})

		t.Run("it adds empty geolocation if the ipaddress is not found in database", func(t *testing.T) {
			t.Parallel()

			inexistentIP := `22.125.160.216`
			input := &types.GatewayBatchRequest{
				RequestIP: inexistentIP,
				Batch: []types.SingularEventT{
					{"userId": "u1", "context": map[string]interface{}{"version": "0.1.0"}},
				},
			}

			err := enricher.Enrich(
				NewSourceBuilder("source-id").
					WithGeoEnrichment(true).
					Build(), input)
			require.Nil(t, err)

			require.Equal(t,
				types.SingularEventT{
					"userId": "u1",
					"context": map[string]interface{}{
						"version": "0.1.0",
						"geo":     Geolocation{IP: inexistentIP},
					},
				},
				input.Batch[0],
			)
		})

		t.Run("it adds geolocation if the ipaddress is found", func(t *testing.T) {
			input := &types.GatewayBatchRequest{
				RequestIP: `2.125.160.216`,
				Batch: []types.SingularEventT{
					{"userId": "u1", "context": map[string]interface{}{"version": "0.1.0"}},
				},
			}

			err := enricher.Enrich(
				NewSourceBuilder("source-id").
					WithGeoEnrichment(true).
					Build(), input)
			require.Nil(t, err)

			require.Equal(t,
				types.SingularEventT{ // expected type with context
					"userId": "u1",
					"context": map[string]interface{}{
						"version": "0.1.0",
						"geo": Geolocation{
							IP:       "2.125.160.216",
							City:     "Boxford",
							Country:  "GB",
							Postal:   "OX1",
							Region:   "England",
							Location: "51.750000,-1.250000",
							Timezone: "Europe/London",
						},
					},
				},
				input.Batch[0], // actual value which was augmented in the batch
			)
		})

		t.Run("it doesn't add geolocation for valid ipaddress if flag for enrichment not enabled", func(t *testing.T) {
			t.Parallel()
			input := &types.GatewayBatchRequest{
				RequestIP: `2.125.160.216`,
				Batch: []types.SingularEventT{
					{
						"anonymousId": "a1",
						"properties": map[string]interface{}{
							"userId": "u1",
						},
						"context": map[string]interface{}{
							"appVersion": "0.1.0",
						},
					},
				},
			}

			err := enricher.Enrich(
				NewSourceBuilder("source-id").
					WithGeoEnrichment(false). // flag for enrichment is `false`
					Build(),
				input,
			)
			require.Nil(t, err)

			// no updates to the entity given we have disabled the flag
			require.Equal(t, types.SingularEventT{
				"anonymousId": "a1",
				"properties": map[string]interface{}{
					"userId": "u1",
				},
				"context": map[string]interface{}{
					"appVersion": "0.1.0",
				},
			}, input.Batch[0])
		})

		t.Run("it adds geolocation even if context section on event is empty", func(t *testing.T) {
			t.Parallel()

			input := &types.GatewayBatchRequest{
				RequestIP: `2.125.160.216`,
				Batch: []types.SingularEventT{
					{
						"anonymousId": "a1",
						"properties": map[string]interface{}{
							"userId": "u1",
						},
					},
				},
			}

			err := enricher.Enrich(
				NewSourceBuilder("source-id").
					WithGeoEnrichment(true).
					Build(), input)
			require.Nil(t, err)

			// In both the events below, we have context section
			// being added or updated to with the geolocation information.
			require.Equal(t, types.SingularEventT{
				"anonymousId": "a1",
				"properties": map[string]interface{}{
					"userId": "u1",
				},
				"context": map[string]interface{}{
					"geo": Geolocation{
						IP:       "2.125.160.216",
						City:     "Boxford",
						Country:  "GB",
						Postal:   "OX1",
						Region:   "England",
						Location: "51.750000,-1.250000",
						Timezone: "Europe/London",
					},
				},
			}, input.Batch[0])
		})

		t.Run("it doesn't add geo information if context is not a map or `geo` key already present in it", func(t *testing.T) {
			t.Parallel()

			input := &types.GatewayBatchRequest{
				RequestIP: `2.125.160.216`,
				Batch: []types.SingularEventT{
					{
						"anonymousId": "a1",
						"properties": map[string]interface{}{
							"userId": "u1",
						},
						"context": map[string]interface{}{
							"geo": "some previous information", // geo key already present
						},
					},
					{
						"anonymousId": "a2",
						"properties": map[string]interface{}{
							"userId": "u2",
						},
						"context": 1.23, // context section is integer and not a map
					},
				},
			}

			err := enricher.Enrich(
				NewSourceBuilder("source-id").
					WithGeoEnrichment(true).
					Build(), input)
			require.NotNil(t, err)

			require.Equal(t, types.SingularEventT{
				"anonymousId": "a1",
				"properties": map[string]interface{}{
					"userId": "u1",
				},
				"context": map[string]interface{}{
					"geo": "some previous information",
				},
			}, input.Batch[0])

			require.Equal(t, types.SingularEventT{
				"anonymousId": "a2",
				"properties": map[string]interface{}{
					"userId": "u2",
				},
				"context": 1.23,
			}, input.Batch[1])
		})

		t.Run("it gives preference to context ip if present and non blank over request ip when adding geolocation", func(t *testing.T) {
			t.Parallel()

			input := &types.GatewayBatchRequest{
				RequestIP: `2.125.160.216`,
				Batch: []types.SingularEventT{
					{
						"anonymousId": "a2",
						"properties": map[string]interface{}{
							"userId": "u2",
						},
						"context": map[string]interface{}{
							"ip": "invalid",
						},
					},
					{
						"anonymousId": "a1",
						"properties": map[string]interface{}{
							"userId": "u1",
						},
						"context": map[string]interface{}{
							"ip": "81.2.69.142",
						},
					},
					{
						"anonymousId": "a3",
						"properties": map[string]interface{}{
							"userId": "u3",
						},
						"context": map[string]interface{}{
							"ip": "",
						},
					},
				},
			}

			err := enricher.Enrich(
				NewSourceBuilder("source-id").
					WithGeoEnrichment(true).
					Build(), input)
			require.Nil(t, err)

			// here the context.ip is present and invalid but the
			// lookup happens on this ip only as non-blank
			require.Equal(t, types.SingularEventT{
				"anonymousId": "a2",
				"properties": map[string]interface{}{
					"userId": "u2",
				},
				"context": map[string]interface{}{
					"geo": Geolocation{IP: "invalid"},
					"ip":  "invalid",
				},
			}, input.Batch[0])

			// Here the context ip is valid and hence lookup
			// will happen on it.
			require.Equal(t, types.SingularEventT{
				"anonymousId": "a1",
				"properties": map[string]interface{}{
					"userId": "u1",
				},
				"context": map[string]interface{}{
					"geo": Geolocation{
						IP:       "81.2.69.142",
						City:     "London",
						Country:  "GB",
						Postal:   "",
						Region:   "England",
						Location: "51.514200,-0.093100",
						Timezone: "Europe/London",
					},
					"ip": "81.2.69.142",
				},
			}, input.Batch[1])

			// In this one, requestIP is fallback as there is
			// context.ip but blank.
			require.Equal(t, types.SingularEventT{
				"anonymousId": "a3",
				"properties": map[string]interface{}{
					"userId": "u3",
				},
				"context": map[string]interface{}{
					"geo": Geolocation{
						IP:       "2.125.160.216",
						City:     "Boxford",
						Country:  "GB",
						Postal:   "OX1",
						Region:   "England",
						Location: "51.750000,-1.250000",
						Timezone: "Europe/London",
					},
					// `context.ip` even though present is blank
					// and hence ip is picked up from request_ip
					"ip": "",
				},
			}, input.Batch[2])
		})
	})
}

func TestMapUpstreamToGeolocation(t *testing.T) {
	t.Run("it returns the extracted fields when input contains all the information", func(t *testing.T) {
		t.Parallel()

		latitude, longitude := float64(0.0002), float64(1.876)

		input := geolocation.GeoInfo{
			City: geolocation.City{
				Names: map[string]string{
					"en": "Gurugram",
				},
			},
			Country: geolocation.Country{
				ISOCode: "IN",
			},
			Postal: geolocation.Postal{
				Code: "122002",
			},
			Location: geolocation.Location{
				Timezone:  "Asia/Kolkata",
				Latitude:  &latitude,
				Longitude: &longitude,
			},
			Subdivisions: []geolocation.Subdivision{
				{Names: map[string]string{"en": "Gurgaon"}},
			},
		}

		actual := extractGeolocationData("1.1.1.1", input)
		require.Equal(t, Geolocation{
			IP:       "1.1.1.1",
			City:     "Gurugram",
			Region:   "Gurgaon",
			Postal:   "122002",
			Location: fmt.Sprintf("%f,%f", latitude, longitude),
			Timezone: "Asia/Kolkata",
			Country:  "IN",
		}, actual)
	})

	t.Run("it returns fields with empty data in case the parent fields are missing", func(t *testing.T) {
		t.Parallel()

		input := geolocation.GeoInfo{}
		actual := extractGeolocationData("1.1.1.1", input)
		require.Equal(t, Geolocation{
			IP:       "1.1.1.1",
			City:     "",
			Region:   "",
			Postal:   "",
			Location: "",
			Timezone: "",
			Country:  "",
		}, actual)
	})
}

func TestDownloadMaxmindDB_success(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	minio, err := miniodocker.Setup(pool, t)
	require.NoError(t, err)

	minioManager, err := filemanager.New(
		&filemanager.Settings{
			Provider: "MINIO",
			Config: map[string]interface{}{
				"bucketName":      minio.BucketName,
				"endPoint":        minio.Endpoint,
				"accessKeyID":     minio.AccessKeyID,
				"secretAccessKey": minio.AccessKeySecret,
			},
			Logger: nil,
			Conf:   nil,
		})
	require.NoError(t, err)

	uploadPath := "./testdata/minio/file.txt"

	f, err := os.Open(uploadPath)
	require.NoError(t, err)

	uploaded, err := minioManager.Upload(context.Background(), f)
	require.NoError(t, err)

	// Once the upload has happened, close the file uploaded
	require.NoError(t, f.Close())

	// Now we can move onto downloading the database from maxmind
	conf := config.New()
	conf.Set("Geolocation.db.key", uploaded.ObjectName)
	conf.Set("Geolocation.db.storage.bucket", minio.BucketName)
	conf.Set("Geolocation.db.storage.endpoint", minio.Endpoint)
	conf.Set("Geolocation.db.storage.accessKey", minio.AccessKeyID)
	conf.Set("Geolocation.db.storage.secretAccessKey", minio.AccessKeySecret)
	conf.Set("Geolocation.db.storage.s3ForcePathStyle", true)
	conf.Set("Geolocation.db.storage.disableSSL", true)

	conf.Set("RUDDER_TMPDIR", t.TempDir())

	downloadPath, err := downloadMaxmindDB(context.Background(), conf, logger.Default.NewLogger())
	require.NoError(t, err)

	equalFiles(t, downloadPath, uploadPath)
	require.NoError(t, os.Remove(downloadPath)) // Clean the downloaded file
}

type SourceBuilder struct {
	source *backendconfig.SourceT
}

func NewSourceBuilder(id string) *SourceBuilder {
	return &SourceBuilder{
		source: &backendconfig.SourceT{
			ID: id,
		},
	}
}

func (sb *SourceBuilder) WithGeoEnrichment(enabled bool) *SourceBuilder {
	sb.source.GeoEnrichment.Enabled = enabled
	return sb
}

func (sb *SourceBuilder) Build() *backendconfig.SourceT {
	return sb.source
}

// equalFiles check if the content of files is equal
func equalFiles(t *testing.T, filepath1, filepath2 string) {
	// the contents of the file should match completely
	byt1, err := os.ReadFile(filepath1)
	require.NoError(t, err)

	byt2, err := os.ReadFile(filepath2)
	require.NoError(t, err)

	require.True(t, bytes.Equal(byt1, byt2))
}

package enricher

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/geolocation"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type GeoTestDBProvider struct{}

func (p *GeoTestDBProvider) GetDB(key, downloadPath string) error {
	return nil
}

func TestGeolocationEnrichment_Setup(t *testing.T) {
	var (
		defaultConf = config.New()
		logger      = logger.NewLogger()
		stats       = stats.Default
	)

	t.Run("inexistent db file causes enricher to fail setup", func(t *testing.T) {
		t.Setenv("Geolocation.db.downloadPath", "./testdata/invalid-db-path")

		_, err := NewGeoEnricher(&GeoTestDBProvider{}, defaultConf, logger, stats)
		require.NotNil(t, err)
		t.Log(err)
		require.True(t, errors.Is(err, geolocation.ErrInvalidDatabase))
	})

	t.Run("corrupted db file causes enricher to fail", func(t *testing.T) {
		t.Setenv("Geolocation.db.downloadPath", "./testdata/corrupted_city_test.mmdb")

		_, err := NewGeoEnricher(&GeoTestDBProvider{}, defaultConf, logger, stats)
		require.NotNil(t, err)
		t.Log(err)
		require.True(t, errors.Is(err, geolocation.ErrInvalidDatabase))
	})
}

func TestGeolocationEnrichment_Success(t *testing.T) {
	c := config.New()
	c.Set("Geolocation.db.downloadPath", "./testdata/city_test.mmdb")
	enricher, err := NewGeoEnricher(&GeoTestDBProvider{}, c, logger.NewLogger(), stats.Default)

	require.Nil(t, err)

	t.Run("it silently returns without enrichment if ip is empty", func(t *testing.T) {
		ip := &types.GatewayBatchRequest{
			RequestIP: ``,
			Batch: []types.SingularEventT{
				{"userId": "1", "context": map[string]interface{}{"app_version": "0.1.0"}},
				{"userId": "2", "context": map[string]interface{}{"app_version": "0.1.0"}},
			},
		}
		// enrichment doesn't happen at all on the context and event remains the same
		err := enricher.Enrich("source-id", ip)
		require.Nil(t, err)
		// require.Equal(t, nil, ip)
		require.Equal(t, types.SingularEventT{"userId": "1", "context": map[string]interface{}{"app_version": "0.1.0"}}, ip.Batch[0])
		require.Equal(t, types.SingularEventT{"userId": "2", "context": map[string]interface{}{"app_version": "0.1.0"}}, ip.Batch[1])
	})

	t.Run("it returns ErrInvalidIP in case ip address passed is malformed", func(t *testing.T) {
		err := enricher.Enrich("source-id", &types.GatewayBatchRequest{
			RequestIP: `invalid-ip`,
			Batch:     nil,
		})

		require.NotNil(t, err)
		require.True(t, errors.Is(err, geolocation.ErrInvalidIP))
	})

	t.Run("it adds empty geolocation if the ipaddress is not found in database", func(t *testing.T) {
		input := &types.GatewayBatchRequest{
			RequestIP: `22.125.160.216`,
			Batch: []types.SingularEventT{
				{"userId": "u1", "context": map[string]interface{}{"version": "0.1.0"}},
			},
		}

		err := enricher.Enrich("source-id", input)
		require.Nil(t, err)

		for _, val := range input.Batch {
			require.Equal(t,
				types.SingularEventT{ // expected type with context
					"userId": "u1",
					"context": map[string]interface{}{
						"version": "0.1.0",
						"geo": &Geolocation{
							City:     "",
							Country:  "",
							Postal:   "",
							Region:   "",
							Location: "",
							Timezone: "",
						},
					},
				},
				val, // actual value which was augmented in the batch
			)
		}
	})

	t.Run("it adds geolocation if the ipaddress is found", func(t *testing.T) {
		input := &types.GatewayBatchRequest{
			RequestIP: `2.125.160.216`,
			Batch: []types.SingularEventT{
				{"userId": "u1", "context": map[string]interface{}{"version": "0.1.0"}},
			},
		}

		err := enricher.Enrich("source-id", input)
		require.Nil(t, err)

		for _, val := range input.Batch {
			require.Equal(t,
				types.SingularEventT{ // expected type with context
					"userId": "u1",
					"context": map[string]interface{}{
						"version": "0.1.0",
						"geo": &Geolocation{
							City:     "Boxford",
							Country:  "GB",
							Postal:   "OX1",
							Region:   "England",
							Location: "51.750000,-1.250000",
							Timezone: "Europe/London",
						},
					},
				},
				val, // actual value which was augmented in the batch
			)
		}
	})

	t.Run("it doesn't add geolocation if the ipaddress is found but context section on event is empty", func(t *testing.T) {
		input := &types.GatewayBatchRequest{
			RequestIP: `2.125.160.216`,
			Batch: []types.SingularEventT{
				{
					"anonymousId": "a1",
					"properties": map[string]interface{}{
						"userId": "u1",
					},
				},
				{
					"anonymousId": "a2",
					"context": map[string]interface{}{
						"version": "0.1.0",
					},
				},
			},
		}

		err := enricher.Enrich("source-id", input)
		require.Nil(t, err)

		// in the first event, we dont have context section and hence
		// no updates to it
		require.Equal(t, types.SingularEventT{
			"anonymousId": "a1",
			"properties": map[string]interface{}{
				"userId": "u1",
			},
		}, input.Batch[0])

		// in the second event as context section present
		// it handles it correctly by adding geolocation data to it.
		require.Equal(t, types.SingularEventT{
			"anonymousId": "a2",
			"context": map[string]interface{}{
				"version": "0.1.0",
				"geo": &Geolocation{
					City:     "Boxford",
					Country:  "GB",
					Postal:   "OX1",
					Region:   "England",
					Location: "51.750000,-1.250000",
					Timezone: "Europe/London",
				},
			},
		}, input.Batch[1])
	})
}

func TestMapUpstreamToGeolocation(t *testing.T) {
	t.Run("it returns the output as nil when input is nil", func(t *testing.T) {
		data := extractGeolocationData(nil)
		require.Nil(t, data)
	})

	t.Run("it returns the extracted fields when input contains all the information", func(t *testing.T) {
		latitude, longitude := float64(0.0002), float64(1.876)

		input := &geolocation.GeoCity{
			City: geolocation.City{
				Names: map[string]string{
					"en": "Gurugram",
				},
			},
			Country: geolocation.Country{
				IsoCode: "IN",
			},
			Postal: geolocation.Postal{
				Code: "122002",
			},
			Location: geolocation.Location{
				TimeZone:  "Asia/Kolkata",
				Latitude:  &latitude,
				Longitude: &longitude,
			},
			Subdivisions: []geolocation.Subdivision{
				{Names: map[string]string{"en": "Gurgaon"}},
			},
		}

		actual := extractGeolocationData(input)
		require.Equal(t, Geolocation{
			City:     "Gurugram",
			Region:   "Gurgaon",
			Postal:   "122002",
			Location: fmt.Sprintf("%f,%f", latitude, longitude),
			Timezone: "Asia/Kolkata",
			Country:  "IN",
		}, *actual)
	})

	t.Run("it returns fields with empty data in case the parent fields are missing", func(t *testing.T) {
		input := &geolocation.GeoCity{}
		actual := extractGeolocationData(input)
		require.Equal(t, Geolocation{
			City:     "",
			Country:  "",
			Postal:   "",
			Location: "",
			Timezone: "",
			Region:   "",
		}, *actual)
	})
}

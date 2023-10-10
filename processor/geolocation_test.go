package processor

import (
	"errors"
	"testing"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/processor/geolocation"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
)

type GeoTestDBProvider struct {
	loc string
}

func (p *GeoTestDBProvider) GetDB(key string) (string, error) {
	return p.loc, nil
}

func TestGeolocationEnrichment_Setup(t *testing.T) {
	var (
		defaultConf = config.New()
		logger      = logger.NewLogger()
		stats       = stats.Default
	)

	t.Run("inexistent db file causes enricher to fail setup", func(t *testing.T) {
		_, err := NewGeoEnricher(&GeoTestDBProvider{loc: "./testdata/invalid-db-path"}, defaultConf, logger, stats)
		require.NotNil(t, err)
		t.Log(err)
		require.True(t, errors.Is(err, geolocation.ErrInvalidDatabase))
	})

	t.Run("corrupted db file causes enricher to fail", func(t *testing.T) {
		_, err := NewGeoEnricher(&GeoTestDBProvider{loc: "./testdata/corrupted_city_input.mmdb"}, defaultConf, logger, stats)
		require.NotNil(t, err)
		t.Log(err)
		require.True(t, errors.Is(err, geolocation.ErrInvalidDatabase))
	})

}

func TestGeolocationEnrichment_Success(t *testing.T) {
	enricher, err := NewGeoEnricher(&GeoTestDBProvider{loc: "./testdata/city_test.mmdb"},
		config.New(), logger.NewLogger(), stats.Default)
	require.Nil(t, err)

	t.Run("it silently returns without enrichment if ip is empty", func(t *testing.T) {
		// construct request with empty ip
		input := getGatewayRequestBatchWithIp(``, map[string]interface{}{"version": "0.1.0"}, 2)
		err := enricher.Enrich("dummy-source-id", input)
		require.Nil(t, err)
		for _, val := range input.Batch {
			require.Equal(t, types.SingularEventT{"context": map[string]interface{}{"version": "0.1.0"}}, val)
		}
	})

	t.Run("it adds empty geolocation if the ipaddress is not found", func(t *testing.T) {
		input := getGatewayRequestBatchWithIp(`22.125.160.216`, map[string]interface{}{"version": "0.1.0"}, 2)
		err := enricher.Enrich("source-id", input)
		require.Nil(t, err)

		for _, val := range input.Batch {
			require.Equal(t,
				types.SingularEventT{ // expected type with context
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
					}},
				val, // actual value which was augmented in the batch
			)
		}
	})

	t.Run("it adds geolocation if the ipaddress is found", func(t *testing.T) {
		input := getGatewayRequestBatchWithIp(`2.125.160.216`, map[string]interface{}{"version": "0.1.0"}, 2)
		err := enricher.Enrich("source-id", input)
		require.Nil(t, err)

		for _, val := range input.Batch {
			require.Equal(t,
				types.SingularEventT{ // expected type with context
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
					}},
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

		for idx, val := range input.Batch {
			// in this event, we dont have context section and hence
			// no updates to it
			if idx == 0 {
				require.Equal(t, types.SingularEventT{
					"anonymousId": "a1",
					"properties": map[string]interface{}{
						"userId": "u1",
					},
				}, val)
			}

			// as context section present in the event
			// it handles it correctly by adding the data to it.
			if idx == 1 {
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
				}, val)
			}
		}
	})
}

func getGatewayRequestBatchWithIp(ip string, context map[string]interface{}, eventCount int) *types.GatewayBatchRequest {
	toReturn := &types.GatewayBatchRequest{
		RequestIP: ip,
		Batch:     make([]types.SingularEventT, eventCount),
	}

	for i := 0; i < eventCount; i++ {
		toReturn.Batch[i] = map[string]interface{}{
			"context": maps.Clone(context),
		}
	}
	return toReturn
}

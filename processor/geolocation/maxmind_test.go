package geolocation_test

import (
	"errors"
	"testing"

	"github.com/rudderlabs/rudder-server/processor/geolocation"
	"github.com/stretchr/testify/require"
)

func TestGeolocationReader_Failure(t *testing.T) {
	t.Run("reader errors out when db is corrupted", func(t *testing.T) {
		_, err := geolocation.NewMaxmindGeoFetcher("./testdata/corrupted_city_test.mmdb")
		require.True(
			t,
			errors.Is(err, geolocation.ErrInvalidDatabase))
	})
}

func TestGeolocationFetcher(t *testing.T) {
	// below data is a test database provided by maxmind for testing purposes
	// https://github.com/maxmind/MaxMind-DB/tree/main/test-data
	f, err := geolocation.NewMaxmindGeoFetcher("./testdata/city_test.mmdb")
	require.Nil(t, err)

	t.Run("fetcher returns error ErrInvalidIP if ip is empty or invalid", func(t *testing.T) {
		lookup, err := f.GeoIP(``)
		require.True(t, errors.Is(err, geolocation.ErrInvalidIP))
		require.Nil(t, lookup)

		_, err = f.GeoIP(`invalid-ip`)
		require.True(t, errors.Is(err, geolocation.ErrInvalidIP))
	})

	t.Run("fetcher returns the geocity data for valid IP present in database", func(t *testing.T) {
		lookup, err := f.GeoIP(`2.125.160.216`) // picked the value from city_test_input.json
		require.Nil(t, err)
		require.Equal(t, map[string]string{"en": "Boxford"}, lookup.City.Names)
		require.Equal(t, `OX1`, lookup.Postal.Code)

		lookup, err = f.GeoIP(`2a02:ff40::0`) // IPv6
		require.Nil(t, err)
		require.Equal(t, `Europe`, lookup.Continent.Names["en"]) // TODO: Do complete matching here.
	})

	t.Run("fetcher returns empty lookup for IP address not available in database", func(t *testing.T) {
		emptyLookup, err := f.GeoIP(`1.1.1.1`)
		require.Nil(t, err)
		require.Equal(t, geolocation.GeoCity{}, *emptyLookup)

		emptyLookup, err = f.GeoIP(`3a02:ff81::0`) // IPv6 lookup
		require.Nil(t, err)
		require.Equal(t, geolocation.GeoCity{}, *emptyLookup)
	})

}

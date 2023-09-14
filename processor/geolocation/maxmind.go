package geolocation

import (
	"fmt"
	"net"

	"github.com/oschwald/maxminddb-golang"
)

type maxmindGeoFetcher struct {
	reader *maxminddb.Reader
}

func NewMaxmindGeoFetcher(dbLoc string) (GeoFetcher, error) {
	reader, err := maxminddb.Open(dbLoc)
	if err != nil {
		return nil, fmt.Errorf("opening maxmind reader from location: %w", err)
	}
	return &maxmindGeoFetcher{reader}, nil
}

func (f *maxmindGeoFetcher) GeoIP(ip net.IP) (Geolocation, error) {
	lookup := City{}
	if err := f.reader.Lookup(ip, &lookup); err != nil {
		return Geolocation{}, fmt.Errorf("reading geolocation for ip: %w", err)
	}

	result := Geolocation{
		City:     lookup.City.Names["en"],
		Country:  lookup.Country.IsoCode,
		Postal:   lookup.Postal.Code,
		Timezone: lookup.Location.TimeZone,
	}

	if len(lookup.Subdivisions) > 0 {
		result.Region = lookup.Subdivisions[0].Names["en"]
	}

	// default values of latitude and longitude can give
	// incorrect result, so we have casted them in pointers so we know
	// when the value is missing.
	if lookup.Location.Latitude != nil && lookup.Location.Longitude != nil {
		result.Location = fmt.Sprintf("%f,%f",
			*lookup.Location.Latitude,
			*lookup.Location.Longitude,
		)
	}

	return result, nil
}

// The City struct corresponds to the data in the GeoIP2/GeoLite2 City
// databases. Given we are using the native library to decode the information,
// we have modified some fields in it to contain the pointer values.
type City struct {
	City struct {
		Names map[string]string `maxminddb:"names"`
	} `maxminddb:"city"`
	Postal struct {
		Code string `maxminddb:"code"`
	} `maxminddb:"postal"`
	Continent struct {
		Names map[string]string `maxminddb:"names"`
		Code  string            `maxminddb:"code"`
	} `maxminddb:"continent"`
	Subdivisions []struct {
		Names   map[string]string `maxminddb:"names"`
		IsoCode string            `maxminddb:"iso_code"`
	} `maxminddb:"subdivisions"`
	Country struct {
		Names   map[string]string `maxminddb:"names"`
		IsoCode string            `maxminddb:"iso_code"`
	} `maxminddb:"country"`
	Location struct {
		TimeZone  string   `maxminddb:"time_zone"`
		Latitude  *float64 `maxminddb:"latitude"`
		Longitude *float64 `maxminddb:"longitude"`
	} `maxminddb:"location"`
}

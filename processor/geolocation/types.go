package geolocation

type GeoFetcher interface {
	GeoIP(ip string) (*GeoCity, error)
}

// The City struct corresponds to the data in the GeoIP2/GeoLite2 City
// databases. Given we are using the native library to decode the information,
// we have modified some fields in it to contain the pointer values.
type GeoCity struct {
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

package geolocation

type GeoFetcher interface {
	Locate(string) (GeoInfo, error)
	Close() error
}

// The City struct corresponds to the data in the GeoIP2/GeoLite2 City
// databases. Given we are using the native library to decode the information,
// we have modified some fields in it to contain the pointer values.
type GeoInfo struct {
	City         City          `maxminddb:"city"`
	Postal       Postal        `maxminddb:"postal"`
	Continent    Continent     `maxminddb:"continent"`
	Subdivisions []Subdivision `maxminddb:"subdivisions"`
	Country      Country       `maxminddb:"country"`
	Location     Location      `maxminddb:"location"`
}

type City struct {
	Names map[string]string `maxminddb:"names"`
}

type Postal struct {
	Code string `maxminddb:"code"`
}

type Continent struct {
	Names map[string]string `maxminddb:"names"`
	Code  string            `maxminddb:"code"`
}

type Subdivision struct {
	Names   map[string]string `maxminddb:"names"`
	ISOCode string            `maxminddb:"iso_code"`
}

type Country struct {
	Names   map[string]string `maxminddb:"names"`
	ISOCode string            `maxminddb:"iso_code"`
}

type Location struct {
	Timezone  string   `maxminddb:"time_zone"`
	Latitude  *float64 `maxminddb:"latitude"`
	Longitude *float64 `maxminddb:"longitude"`
}

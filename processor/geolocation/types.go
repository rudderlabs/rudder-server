package geolocation

import "net"

type GeoFetcher interface {
	GeoIP(ip net.IP) (Geolocation, error)
}

type Geolocation struct {
	City     string `json:"city"`
	Country  string `json:"country"`
	Region   string `json:"region"`
	Postal   string `json:"postal"`
	Location string `json:"location"`
	Timezone string `json:"timezone"`
}

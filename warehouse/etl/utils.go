package etl

import (
	"net"

	"github.com/rudderlabs/rudder-server/utils/logger"
)

func AssertError(iface interface{}, err error) {
	if err != nil {
		logger.Fatal(err, iface)
		panic(err)
	}
}

func AssertString(iface interface{}, errorString string) {
	logger.Fatal(errorString, iface)
	panic(errorString)
}

func GetMyIp() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "Ip Empty"
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return "Ip Empty"
}

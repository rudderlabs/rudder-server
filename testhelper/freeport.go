package testhelper

import (
	"sync"

	"github.com/phayes/freeport"
)

var (
	usedPorts   map[int]struct{}
	usedPortsMu sync.Mutex
)

func GetFreePort() (int, error) {
	usedPortsMu.Lock()
	defer usedPortsMu.Unlock()
	for {
		port, err := freeport.GetFreePort()
		if err != nil {
			return 0, err
		}

		if usedPorts == nil {
			usedPorts = make(map[int]struct{})
		}
		if _, used := usedPorts[port]; used {
			continue
		}
		usedPorts[port] = struct{}{}
		return port, nil
	}
}

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
	for {
		port, err := freeport.GetFreePort()
		if err != nil {
			return 0, err
		}

		usedPortsMu.Lock()
		if usedPorts == nil {
			usedPorts = make(map[int]struct{})
		}
		if _, used := usedPorts[port]; used {
			usedPortsMu.Unlock()
			continue
		}
		usedPorts[port] = struct{}{}
		usedPortsMu.Unlock()
		return port, nil
	}
}

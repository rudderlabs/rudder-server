package gateway

type GatewayAdmin struct {
	handle *HandleT
}

// Status function is used for debug purposes by the admin interface
func (g *GatewayAdmin) Status() interface{} {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()
	writeKeys := make([]string, 0, len(enabledWriteKeysSourceMap))
	for k := range enabledWriteKeysSourceMap {
		writeKeys = append(writeKeys, k)
	}

	return map[string]interface{}{
		"ack-count":          g.handle.ackCount,
		"recv-count":         g.handle.recvCount,
		"enabled-write-keys": writeKeys,
		"jobsdb":             g.handle.jobsDB.Status(),
	}

}

package gateway

import (
	"fmt"
	"strings"
)

type GatewayAdmin struct {
	handle *HandleT
}

// Status function is used for debug purposes by the admin interface
func (g *GatewayAdmin) Status() string {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()
	writeKeys := make([]string, 0, len(enabledWriteKeysSourceMap))
	for k := range enabledWriteKeysSourceMap {
		writeKeys = append(writeKeys, k)
	}
	return fmt.Sprintf(
		`Gateway:
---------
Ack Count  : %d
Recv Count : %d
Enabled write keys:
 %s`,
		g.handle.ackCount,
		g.handle.recvCount,
		strings.Join(writeKeys, "\n "))
}

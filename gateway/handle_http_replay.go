package gateway

import "net/http"

// webReplayHandler can handle replay requests
func (gw *Handle) webReplayHandler() http.HandlerFunc {
	return gw.callType("replay", gw.replaySourceIDAuth(gw.webHandler()))
}

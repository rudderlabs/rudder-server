package gateway

import "net/http"

// webImportHandler can handle import requests
func (gw *Handle) webReplayHandler() http.HandlerFunc {
	return gw.callType("replay", gw.replaySourceIDAuth(gw.webHandler()))
}

package gateway

import "net/http"

// webImportHandler can handle import requests
func (gw *Handle) webReplayHandler() http.HandlerFunc {
	return gw.callType("batch", gw.replaySourceIDAuth(func(w http.ResponseWriter, r *http.Request) {
		gw.webRequestHandler(gw.rrh, w, r)
	}))
}

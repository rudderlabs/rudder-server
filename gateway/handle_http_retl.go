package gateway

import "net/http"

// webRetlHandler - handler for retl requests
func (gw *Handle) webRetlHandler() http.HandlerFunc {
	return gw.callType("retl", gw.sourceDestIDAuth(gw.webHandler()))
}

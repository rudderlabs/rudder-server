package gateway

import "net/http"

// webImportHandler can handle import requests
func (gw *Handle) webImportHandler() http.HandlerFunc {
	return gw.callType("import", gw.writeKeyAuth(func(w http.ResponseWriter, r *http.Request) {
		gw.webRequestHandler(gw.irh, w, r)
	}))
}

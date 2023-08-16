package gateway

import (
	"context"
	"net/http"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	gwstats "github.com/rudderlabs/rudder-server/gateway/internal/stats"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// writeKeyAuth middleware to authenticate writeKey in the Authorization header.
// If the writeKey is valid and the source is enabled, the source auth info is added to the request context.
// If the writeKey is invalid, the request is rejected.
func (gw *Handle) writeKeyAuth(delegate http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqType := r.Context().Value(gwtypes.CtxParamCallType).(string)
		var errorMessage string
		defer func() {
			gw.handleHttpError(w, r, errorMessage)
		}()
		writeKey, _, ok := r.BasicAuth()
		if !ok || writeKey == "" {
			stat := gwstats.SourceStat{
				Source:   "noWriteKey",
				SourceID: "noWriteKey",
				WriteKey: "noWriteKey",
				ReqType:  reqType,
			}
			stat.RequestFailed("noWriteKeyInBasicAuth")
			stat.Report(gw.stats)
			errorMessage = response.NoWriteKeyInBasicAuth
			return
		}
		arctx := gw.authRequestContextForWriteKey(writeKey)
		if arctx == nil {
			errorMessage = response.InvalidWriteKey
			return
		}
		if !arctx.SourceEnabled {
			errorMessage = response.SourceDisabled
			return
		}
		augmentAuthRequestContext(arctx, r)
		delegate.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), gwtypes.CtxParamAuthRequestContext, arctx)))
	}
}

// webhookAuth middleware to authenticate webhook requests.
// The writeKey can be passed in the Authorization header or as a query param.
// If the writeKey is valid, corresponds to a webhook source and the source is enabled, the source auth info is added to the request context.
// If the writeKey is invalid, the request is rejected.
func (gw *Handle) webhookAuth(delegate http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqType := r.Context().Value(gwtypes.CtxParamCallType).(string)
		var errorMessage string
		defer func() {
			gw.handleHttpError(w, r, errorMessage)
		}()

		var writeKey string
		if writeKeys, found := r.URL.Query()["writeKey"]; found && writeKeys[0] != "" {
			writeKey = writeKeys[0]
		} else {
			writeKey, _, _ = r.BasicAuth()
		}
		if writeKey == "" {
			stat := gwstats.SourceStat{
				Source:   "noWriteKey",
				SourceID: "noWriteKey",
				WriteKey: "noWriteKey",
				ReqType:  reqType,
			}
			stat.RequestFailed("noWriteKeyInQueryParams")
			stat.Report(gw.stats)
			errorMessage = response.NoWriteKeyInQueryParams
			return
		}
		arctx := gw.authRequestContextForWriteKey(writeKey)
		if arctx == nil || arctx.SourceCategory != "webhook" {
			errorMessage = response.InvalidWriteKey
			return
		}
		if !arctx.SourceEnabled {
			errorMessage = response.SourceDisabled
			return
		}
		augmentAuthRequestContext(arctx, r)
		delegate.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), gwtypes.CtxParamAuthRequestContext, arctx)))
	}
}

// sourceIDAuth middleware to authenticate sourceID in the X-Rudder-Source-Id header.
// If the sourceID is valid and the source is enabled, the source auth info is added to the request context.
// If the sourceID is invalid, the request is rejected.
func (gw *Handle) sourceIDAuth(delegate http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqType := r.Context().Value(gwtypes.CtxParamCallType).(string)
		var errorMessage string
		defer func() {
			gw.handleHttpError(w, r, errorMessage)
		}()
		sourceID := r.Header.Get("X-Rudder-Source-Id")
		if sourceID == "" {
			stat := gwstats.SourceStat{
				Source:   "noSourceID",
				SourceID: "noSourceID",
				WriteKey: "noSourceID",
				ReqType:  reqType,
			}
			stat.RequestFailed("noSourceIdInHeader")
			stat.Report(gw.stats)
			errorMessage = response.NoSourceIdInHeader
			return
		}
		arctx := gw.authRequestContextForSourceID(sourceID)
		if arctx == nil {
			errorMessage = response.InvalidSourceID
			return
		}
		if !arctx.SourceEnabled {
			errorMessage = response.SourceDisabled
			return
		}
		augmentAuthRequestContext(arctx, r)
		delegate.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), gwtypes.CtxParamAuthRequestContext, arctx)))
	}
}

// replaySourceIDAuth middleware to authenticate sourceID in the X-Rudder-Source-Id header.
// If the sourceID is valid, i.e. it is a replay source and enabled, the source auth info is added to the request context.
// If the sourceID is invalid, the request is rejected.
func (gw *Handle) replaySourceIDAuth(delegate http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqType := r.Context().Value(gwtypes.CtxParamCallType).(string)
		var errorMessage string
		defer func() {
			if errorMessage != "" {
				status := response.GetErrorStatusCode(errorMessage)
				responseBody := response.GetStatus(errorMessage)
				gw.logger.Infow("response",
					"ip", misc.GetIPFromReq(r),
					"path", r.URL.Path,
					"status", status,
					"body", responseBody)
				http.Error(w, responseBody, status)
			}
		}()
		sourceID := r.Header.Get("X-Rudder-Source-Id")
		if sourceID == "" {
			stat := gwstats.SourceStat{
				Source:   "noSourceID",
				SourceID: "noSourceID",
				WriteKey: "noSourceID",
				ReqType:  reqType,
			}
			stat.RequestFailed("noSourceIdInHeader")
			stat.Report(gw.stats)
			errorMessage = response.NoWriteKeyInBasicAuth
			return
		}
		arctx := gw.authRequestContextForReplaySourceID(sourceID)
		if arctx == nil {
			errorMessage = response.InvalidReplaySource
			return
		}
		if !arctx.SourceEnabled {
			errorMessage = response.SourceDisabled
			return
		}
		augmentAuthRequestContext(arctx, r)
		delegate.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), gwtypes.CtxParamAuthRequestContext, arctx)))
	}
}

// augmentAuthRequestContext adds source job run id and task run id from the request to the authentication context.
func augmentAuthRequestContext(arctx *gwtypes.AuthRequestContext, r *http.Request) {
	arctx.SourceJobRunID = r.Header.Get("X-Rudder-Job-Run-Id")
	arctx.SourceTaskRunID = r.Header.Get("X-Rudder-Task-Run-Id")
}

// authRequestContextForSourceID gets request context for a given sourceID. If the sourceID is invalid, returns nil.
func (gw *Handle) authRequestContextForSourceID(sourceID string) *gwtypes.AuthRequestContext {
	gw.configSubscriberLock.RLock()
	defer gw.configSubscriberLock.RUnlock()
	if s, ok := gw.sourceIDSourceMap[sourceID]; ok {
		return sourceToRequestContext(s)
	}
	return nil
}

// authRequestContextForReplaySourceID gets request context for a given replay sourceID. If the sourceID is invalid, returns nil.
func (gw *Handle) authRequestContextForReplaySourceID(sourceID string) *gwtypes.AuthRequestContext {
	gw.configSubscriberLock.RLock()
	defer gw.configSubscriberLock.RUnlock()
	if s, ok := gw.replaySourceIDSourceMap[sourceID]; ok {
		return sourceToRequestContext(s)
	}
	return nil
}

// authRequestContextForWriteKey gets request context for a given writeKey. If the writeKey is invalid, returns nil.
func (gw *Handle) authRequestContextForWriteKey(writeKey string) *gwtypes.AuthRequestContext {
	gw.configSubscriberLock.RLock()
	defer gw.configSubscriberLock.RUnlock()
	if s, ok := gw.writeKeysSourceMap[writeKey]; ok {
		return sourceToRequestContext(s)
	}
	return nil
}

// sourceToRequestContext converts a source to request context.
func sourceToRequestContext(s backendconfig.SourceT) *gwtypes.AuthRequestContext {
	arctx := &gwtypes.AuthRequestContext{
		SourceEnabled:  s.Enabled,
		SourceID:       s.ID,
		WriteKey:       s.WriteKey,
		WorkspaceID:    s.WorkspaceID,
		SourceName:     s.Name,
		SourceCategory: s.SourceDefinition.Category,
		SourceDefName:  s.SourceDefinition.Name,
	}
	if arctx.SourceCategory == "" {
		arctx.SourceCategory = eventStreamSourceCategory
	}
	return arctx
}

func (gw *Handle) handleHttpError(w http.ResponseWriter, r *http.Request, errorMessage string) {
	if errorMessage != "" {
		status := response.GetErrorStatusCode(errorMessage)
		responseBody := response.GetStatus(errorMessage)
		gw.logger.Infow("response",
			"ip", misc.GetIPFromReq(r),
			"path", r.URL.Path,
			"status", status,
			"body", responseBody)
		http.Error(w, responseBody, status)
	}
}

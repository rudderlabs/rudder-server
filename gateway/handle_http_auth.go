package gateway

import (
	"context"
	"net/http"

	"github.com/samber/lo"

	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"

	gwCtx "github.com/rudderlabs/rudder-server/gateway/internal/context"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	gwstats "github.com/rudderlabs/rudder-server/gateway/internal/stats"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
	"github.com/rudderlabs/rudder-server/gateway/response"
)

// writeKeyAuth middleware to authenticate writeKey in the Authorization header.
// If the writeKey is valid and the source is enabled, the source auth info is added to the request context.
// If the writeKey is invalid, the request is rejected.
func (gw *Handle) writeKeyAuth(delegate http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqType := r.Context().Value(gwtypes.CtxParamCallType).(string)
		var errorMessage string
		var arctx *gwtypes.AuthRequestContext
		defer func() {
			gw.handleHttpError(w, r, errorMessage)
			gw.handleFailureStats(errorMessage, reqType, arctx)
		}()
		writeKey, _, ok := r.BasicAuth()
		if !ok || writeKey == "" {
			errorMessage = response.NoWriteKeyInBasicAuth
			return
		}
		arctx = gw.authRequestContextForWriteKey(writeKey)
		if arctx == nil {
			stat := gwstats.SourceStat{
				Source:   "invalidWriteKey",
				SourceID: "invalidWriteKey",
				WriteKey: writeKey,
				ReqType:  reqType,
			}
			stat.RequestFailed("invalidWriteKey")
			stat.Report(gw.stats)
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
		var arctx *gwtypes.AuthRequestContext
		var errorMessage string
		defer func() {
			gw.handleHttpError(w, r, errorMessage)
			gw.handleFailureStats(errorMessage, reqType, arctx)
		}()

		var writeKey string
		if writeKeys, found := r.URL.Query()["writeKey"]; found && writeKeys[0] != "" {
			writeKey = writeKeys[0]
		} else {
			writeKey, _, _ = r.BasicAuth()
		}
		if writeKey == "" {
			errorMessage = response.NoWriteKeyInQueryParams
			return
		}
		arctx = gw.authRequestContextForWriteKey(writeKey)
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
		var arctx *gwtypes.AuthRequestContext
		defer func() {
			gw.handleHttpError(w, r, errorMessage)
			gw.handleFailureStats(errorMessage, reqType, arctx)
		}()
		sourceID := r.Header.Get("X-Rudder-Source-Id")
		if sourceID == "" {
			errorMessage = response.NoSourceIdInHeader
			return
		}
		arctx = gw.authRequestContextForSourceID(sourceID)
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

// authDestIDForSource middleware to authenticate destinationId in the X-Rudder-Destination-Id header.
// If the destinationId is invalid, the request is rejected.
// destinationID authentication should be performed only after source is authenticated and source is present in context
// Following validations are performed
//  1. Destination should be present for source config
//  2. Destination should be enabled for the source
func (gw *Handle) authDestIDForSource(delegate http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var errorMessage, reqType string
		var arctx *gwtypes.AuthRequestContext
		defer func() {
			gw.handleHttpError(w, r, errorMessage)
			gw.handleFailureStats(errorMessage, reqType, arctx)
		}()
		reqType, ok := gwCtx.GetRequestTypeFromCtx(r.Context())
		if !ok {
			errorMessage = "unable to get request type from context"
			return
		}
		arctx, ok = gwCtx.GetAuthRequestFromCtx(r.Context())
		if !ok {
			errorMessage = "unable to get AuthRequest from context"
			return
		}

		destinationID := r.Header.Get("X-Rudder-Destination-Id")
		if destinationID == "" {
			// TODO: make default value true once rETL team migrates to sending destination ID in header
			if !gw.config.GetBool("Gateway.requireDestinationIdHeader", false) {
				delegate.ServeHTTP(w, r)
				return
			}
			errorMessage = response.NoDestinationIDInHeader
			return
		}
		destination, found := lo.Find(arctx.Source.Destinations, func(dest backendconfig.DestinationT) bool {
			return dest.ID == destinationID
		})
		if !found {
			errorMessage = response.InvalidDestinationID
			return
		}
		if !destination.Enabled {
			errorMessage = response.DestinationDisabled
			return
		}
		arctx.DestinationID = destinationID
		delegate.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), gwtypes.CtxParamAuthRequestContext, arctx)))
	}
}

// replaySourceIDAuth middleware to authenticate sourceID in the X-Rudder-Source-Id header.
// If the sourceID is valid, i.e. it is a replay source and enabled, the source auth info is added to the request context.
// If the sourceID is invalid, the request is rejected.
func (gw *Handle) replaySourceIDAuth(delegate http.HandlerFunc) http.HandlerFunc {
	return gw.sourceIDAuth(func(w http.ResponseWriter, r *http.Request) {
		arctx := r.Context().Value(gwtypes.CtxParamAuthRequestContext).(*gwtypes.AuthRequestContext)
		s, ok := gw.sourceIDSourceMap[arctx.SourceID]
		if !ok || !s.IsReplaySource() {
			gw.handleHttpError(w, r, response.InvalidReplaySource)
			gw.handleFailureStats(response.InvalidReplaySource, "replay", arctx)
			return
		}
		delegate.ServeHTTP(w, r)
	})
}

// sourceDestIDAuth middleware to authenticate sourceID and destinationID
// in the X-Rudder-Source-Id and X-Rudder-Destination-Id header respectively.
// If the sourceID or destinationID is invalid, the request is rejected.
func (gw *Handle) sourceDestIDAuth(delegate http.HandlerFunc) http.HandlerFunc {
	return gw.sourceIDAuth(gw.authDestIDForSource(delegate))
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
		ReplaySource:   s.IsReplaySource(),
		Source:         s,
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
			"ip", kithttputil.GetRequestIP(r),
			"path", r.URL.Path,
			"status", status,
			"body", responseBody)
		http.Error(w, responseBody, status)
	}
}

func (gw *Handle) handleFailureStats(errorMessage, reqType string, arctx *gwtypes.AuthRequestContext) {
	if errorMessage != "" {
		var stat gwstats.SourceStat
		switch errorMessage {
		case response.NoWriteKeyInBasicAuth, response.NoWriteKeyInQueryParams:
			stat = gwstats.SourceStat{
				Source:   "noWriteKey",
				SourceID: "noWriteKey",
				WriteKey: "noWriteKey",
				ReqType:  reqType,
			}
		case response.InvalidWriteKey:
			stat = gwstats.SourceStat{
				Source:   "noWriteKey",
				SourceID: "noWriteKey",
				WriteKey: "noWriteKey",
				ReqType:  reqType,
			}
		case response.InvalidSourceID:
			stat = gwstats.SourceStat{
				SourceID: "InvalidSourceId",
				WriteKey: "InvalidSourceId",
				ReqType:  reqType,
				Source:   "InvalidSourceId",
			}
		case response.NoSourceIdInHeader:
			stat = gwstats.SourceStat{
				SourceID: "noSourceIDInHeader",
				WriteKey: "noSourceIDInHeader",
				ReqType:  reqType,
				Source:   "noSourceIDInHeader",
			}
		case response.SourceDisabled, response.NoDestinationIDInHeader, response.InvalidDestinationID, response.DestinationDisabled:
			stat = gwstats.SourceStat{
				SourceID:    arctx.SourceID,
				WriteKey:    arctx.WriteKey,
				ReqType:     reqType,
				Source:      arctx.SourceTag(),
				WorkspaceID: arctx.WorkspaceID,
				SourceType:  arctx.SourceCategory,
			}
		}
		stat.RequestFailed(response.GetStatus(errorMessage))
		stat.Report(gw.stats)
	}
}

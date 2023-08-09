package gateway

import "github.com/rudderlabs/rudder-server/utils/misc"

func (gw *Handle) getSourceTagFromWriteKey(writeKey string) string {
	sourceName := gw.getSourceNameForWriteKey(writeKey)
	sourceTag := misc.GetTagName(writeKey, sourceName)
	return sourceTag
}

func (gw *Handle) getSourceCategoryForWriteKey(writeKey string) (category string) {
	gw.conf.configSubscriberLock.RLock()
	defer gw.conf.configSubscriberLock.RUnlock()

	if _, ok := gw.conf.writeKeysSourceMap[writeKey]; ok {
		category = gw.conf.writeKeysSourceMap[writeKey].SourceDefinition.Category
		if category == "" {
			category = eventStreamSourceCategory
		}
	}
	return
}

func (gw *Handle) getWorkspaceForWriteKey(writeKey string) string {
	gw.conf.configSubscriberLock.RLock()
	defer gw.conf.configSubscriberLock.RUnlock()

	if _, ok := gw.conf.enabledWriteKeyWorkspaceMap[writeKey]; ok {
		return gw.conf.enabledWriteKeyWorkspaceMap[writeKey]
	}
	return ""
}

func (gw *Handle) isValidWriteKey(writeKey string) bool {
	gw.conf.configSubscriberLock.RLock()
	defer gw.conf.configSubscriberLock.RUnlock()

	_, ok := gw.conf.writeKeysSourceMap[writeKey]
	return ok
}

func (gw *Handle) isWriteKeyEnabled(writeKey string) bool {
	gw.conf.configSubscriberLock.RLock()
	defer gw.conf.configSubscriberLock.RUnlock()
	if source, ok := gw.conf.writeKeysSourceMap[writeKey]; ok {
		return source.Enabled
	}
	return false
}

func (gw *Handle) getSourceIDForWriteKey(writeKey string) string {
	gw.conf.configSubscriberLock.RLock()
	defer gw.conf.configSubscriberLock.RUnlock()

	if _, ok := gw.conf.writeKeysSourceMap[writeKey]; ok {
		return gw.conf.writeKeysSourceMap[writeKey].ID
	}

	return ""
}

func (gw *Handle) getSourceNameForWriteKey(writeKey string) string {
	gw.conf.configSubscriberLock.RLock()
	defer gw.conf.configSubscriberLock.RUnlock()

	if _, ok := gw.conf.writeKeysSourceMap[writeKey]; ok {
		return gw.conf.writeKeysSourceMap[writeKey].Name
	}

	return "-notFound-"
}

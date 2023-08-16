package gateway

import "github.com/rudderlabs/rudder-server/utils/misc"

func (gateway *Handle) getSourceTagFromWriteKey(writeKey string) string {
	sourceName := gateway.getSourceNameForWriteKey(writeKey)
	sourceTag := misc.GetTagName(writeKey, sourceName)
	return sourceTag
}

func (gateway *Handle) getSourceCategoryForWriteKey(writeKey string) (category string) {
	gateway.conf.configSubscriberLock.RLock()
	defer gateway.conf.configSubscriberLock.RUnlock()

	if _, ok := gateway.conf.writeKeysSourceMap[writeKey]; ok {
		category = gateway.conf.writeKeysSourceMap[writeKey].SourceDefinition.Category
		if category == "" {
			category = eventStreamSourceCategory
		}
	}
	return
}

func (gateway *Handle) getWorkspaceForWriteKey(writeKey string) string {
	gateway.conf.configSubscriberLock.RLock()
	defer gateway.conf.configSubscriberLock.RUnlock()

	if _, ok := gateway.conf.enabledWriteKeyWorkspaceMap[writeKey]; ok {
		return gateway.conf.enabledWriteKeyWorkspaceMap[writeKey]
	}
	return ""
}

func (gateway *Handle) isValidWriteKey(writeKey string) bool {
	gateway.conf.configSubscriberLock.RLock()
	defer gateway.conf.configSubscriberLock.RUnlock()

	_, ok := gateway.conf.writeKeysSourceMap[writeKey]
	return ok
}

func (gateway *Handle) isWriteKeyEnabled(writeKey string) bool {
	gateway.conf.configSubscriberLock.RLock()
	defer gateway.conf.configSubscriberLock.RUnlock()
	if source, ok := gateway.conf.writeKeysSourceMap[writeKey]; ok {
		return source.Enabled
	}
	return false
}

func (gateway *Handle) getSourceIDForWriteKey(writeKey string) string {
	gateway.conf.configSubscriberLock.RLock()
	defer gateway.conf.configSubscriberLock.RUnlock()

	if _, ok := gateway.conf.writeKeysSourceMap[writeKey]; ok {
		return gateway.conf.writeKeysSourceMap[writeKey].ID
	}

	return ""
}

func (gateway *Handle) getSourceNameForWriteKey(writeKey string) string {
	gateway.conf.configSubscriberLock.RLock()
	defer gateway.conf.configSubscriberLock.RUnlock()

	if _, ok := gateway.conf.writeKeysSourceMap[writeKey]; ok {
		return gateway.conf.writeKeysSourceMap[writeKey].Name
	}

	return "-notFound-"
}

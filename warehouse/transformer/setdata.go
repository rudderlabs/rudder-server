package transformer

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/rules"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
)

type prefixInfo struct {
	completePrefix string
	completeLevel  int
	prefix         string
	level          int
}

func (t *transformer) setDataAndColumnTypeFromInput(
	processInfo *processingInfo, input any,
	data map[string]any, columnType map[string]string,
	prefixDetails *prefixInfo,
) error {
	if !utils.IsObject(input) {
		return nil
	}

	inputMap := input.(map[string]any)

	if len(inputMap) == 0 {
		return nil
	}
	if t.shouldHandleStringLikeObject(prefixDetails, inputMap) {
		return t.handleStringLikeObject(processInfo, prefixDetails, data, columnType, inputMap)
	}
	for key, val := range inputMap {
		if val == nil || utils.IsBlank(val) {
			continue
		}

		if t.isValidJSONPath(processInfo, prefixDetails, key) {
			if err := t.handleValidJSONPath(processInfo, prefixDetails, key, val, data, columnType); err != nil {
				return fmt.Errorf("handling valid JSON path: %w", err)
			}
		} else if t.shouldProcessNestedObject(processInfo, prefixDetails, val) {
			if err := t.processNestedObject(processInfo, val.(map[string]any), data, columnType, prefixDetails, key); err != nil {
				return fmt.Errorf("processing nested object: %w", err)
			}
		} else {
			if err := t.processNonNestedObject(processInfo, prefixDetails, key, val, data, columnType); err != nil {
				return fmt.Errorf("handling non-nested object: %w", err)
			}
		}
	}
	return nil
}

func (t *transformer) shouldHandleStringLikeObject(prefixDetails *prefixInfo, inputMap map[string]any) bool {
	return (strings.HasSuffix(prefixDetails.completePrefix, "context_traits_") || prefixDetails.completePrefix == "group_traits_") && utils.IsStringLikeObject(inputMap)
}

func (t *transformer) handleStringLikeObject(processInfo *processingInfo, prefixDetails *prefixInfo, data map[string]any, columnType map[string]string, inputMap map[string]any) error {
	if prefixDetails.prefix == "context_traits_" {
		err := t.addColumnTypeAndValue(processInfo, prefixDetails.prefix, utils.StringLikeObjectToString(inputMap), false, data, columnType)
		if err != nil {
			return fmt.Errorf("adding column type and value: %w", err)
		}
		return nil
	}
	return nil
}

func (t *transformer) isValidJSONPath(processInfo *processingInfo, prefixDetails *prefixInfo, key string) bool {
	validLegacyJSONPath := isValidLegacyJSONPathKey(processInfo.event.Metadata.EventType, prefixDetails.prefix+key, prefixDetails.level, processInfo.jsonPathsInfo.legacyKeysMap)
	validJSONPath := isValidJSONPathKey(prefixDetails.completePrefix+key, prefixDetails.completeLevel, processInfo.jsonPathsInfo.keysMap)
	return validLegacyJSONPath || validJSONPath
}

func (t *transformer) handleValidJSONPath(processInfo *processingInfo, prefixDetails *prefixInfo, key string, val any, data map[string]any, columnType map[string]string) error {
	valJSON, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("marshalling value: %w", err)
	}
	return t.addColumnTypeAndValue(processInfo, prefixDetails.prefix+key, valJSON, true, data, columnType)
}

func (t *transformer) shouldProcessNestedObject(processInfo *processingInfo, prefixDetails *prefixInfo, val any) bool {
	return utils.IsObject(val) && (processInfo.event.Metadata.SourceCategory != "cloud" || prefixDetails.level < 3)
}

func (t *transformer) processNestedObject(
	processInfo *processingInfo, val map[string]any,
	data map[string]any, columnType map[string]string,
	prefixDetails *prefixInfo, key string,
) error {
	newPrefixDetails := &prefixInfo{
		completePrefix: prefixDetails.completePrefix + key + "_",
		completeLevel:  prefixDetails.completeLevel + 1,
		prefix:         prefixDetails.prefix + key + "_",
		level:          prefixDetails.level + 1,
	}
	return t.setDataAndColumnTypeFromInput(processInfo, val, data, columnType, newPrefixDetails)
}

func (t *transformer) processNonNestedObject(processInfo *processingInfo, prefixDetails *prefixInfo, key string, val any, data map[string]any, columnType map[string]string) error {
	tempData := val
	if processInfo.event.Metadata.SourceCategory == "cloud" && prefixDetails.level >= 3 && utils.IsObject(val) {
		var err error
		tempData, err = json.Marshal(val)
		if err != nil {
			return fmt.Errorf("marshalling value: %w", err)
		}
	}
	return t.addColumnTypeAndValue(processInfo, prefixDetails.prefix+key, tempData, false, data, columnType)
}

func (t *transformer) addColumnTypeAndValue(pi *processingInfo, key string, val any, isJSONKey bool, data map[string]any, columnType map[string]string) error {
	columnName := TransformColumnName(pi.event.Metadata.DestinationType, pi.itrOpts, pi.dstOpts, key)
	if len(columnName) == 0 {
		return nil
	}

	safeKey, err := SafeColumnName(pi.event.Metadata.DestinationType, pi.itrOpts, columnName)
	if err != nil {
		return fmt.Errorf("transforming column name: %w", err)
	}

	if rules.IsRudderReservedColumn(pi.event.Metadata.EventType, safeKey) {
		return nil
	}

	data[safeKey] = val
	columnType[safeKey] = t.getDataType(pi.event.Metadata.DestinationType, key, val, isJSONKey)
	return nil
}

func (t *transformer) setDataAndColumnTypeFromRules(
	pi *processingInfo,
	data map[string]any, columnType map[string]string,
	rules map[string]string, functionalRules map[string]rules.FunctionalRules,
) error {
	if err := t.setFromRules(pi, data, columnType, rules); err != nil {
		return fmt.Errorf("setting data and column type from rules: %w", err)
	}
	if err := t.setFromFunctionalRules(pi, data, columnType, functionalRules); err != nil {
		return fmt.Errorf("setting data and column type from functional rules: %w", err)
	}
	return nil
}

func (t *transformer) setFromRules(
	pi *processingInfo,
	data map[string]any, columnType map[string]string,
	rules map[string]string,
) error {
	for colKey, valKey := range rules {
		columnName, err := SafeColumnName(pi.event.Metadata.DestinationType, pi.itrOpts, colKey)
		if err != nil {
			return fmt.Errorf("safe column name: %w", err)
		}

		delete(data, columnName)
		delete(columnType, columnName)

		colVal := misc.MapLookup(pi.event.Message, strings.Split(valKey, ".")...)
		if colVal == nil || utils.IsBlank(colVal) || utils.IsObject(colVal) {
			continue
		}

		data[columnName] = colVal
		columnType[columnName] = t.getDataType(pi.event.Metadata.DestinationType, colKey, colVal, false)
	}
	return nil
}

func (t *transformer) setFromFunctionalRules(
	pi *processingInfo,
	data map[string]any, columnType map[string]string,
	functionalRules map[string]rules.FunctionalRules,
) error {
	for colKey, functionalRule := range functionalRules {
		columnName, err := SafeColumnName(pi.event.Metadata.DestinationType, pi.itrOpts, colKey)
		if err != nil {
			return fmt.Errorf("safe column name: %w", err)
		}

		delete(data, columnName)
		delete(columnType, columnName)

		colVal, err := functionalRule(pi.event)
		if err != nil {
			return fmt.Errorf("applying functional rule: %w", err)
		}
		if colVal == nil || utils.IsBlank(colVal) || utils.IsObject(colVal) {
			continue
		}

		data[columnName] = colVal
		columnType[columnName] = t.getDataType(pi.event.Metadata.DestinationType, colKey, colVal, false)
	}
	return nil
}

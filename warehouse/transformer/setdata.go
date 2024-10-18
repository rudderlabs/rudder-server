package transformer

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/rules"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
)

func (t *transformer) setDataAndColumnTypeFromInput(
	pi *processingInfo, input any,
	data map[string]any, columnType map[string]string,
	completePrefix string, completeLevel int,
	prefix string, level int,
) error {
	if !utils.IsObject(input) {
		return nil
	}

	inputMap := input.(map[string]any)

	if len(inputMap) == 0 {
		return nil
	}
	if (strings.HasSuffix(completePrefix, "context_traits_") || completePrefix == "group_traits_") && utils.IsStringLikeObject(inputMap) {
		if prefix == "context_traits_" {
			err := t.addColumnTypeAndValue(pi, prefix, utils.StringLikeObjectToString(inputMap), false, data, columnType)
			if err != nil {
				return fmt.Errorf("adding column type and value: %w", err)
			}
			return nil
		}
		return nil
	}
	for key, val := range inputMap {
		if val == nil || utils.IsBlank(val) {
			continue
		}

		validLegacyJSONPath := isValidLegacyJSONPathKey(pi.event.Metadata.EventType, prefix+key, level, pi.jsonPathsInfo.legacyKeysMap)
		validJSONPath := isValidJSONPathKey(completePrefix+key, completeLevel, pi.jsonPathsInfo.keysMap)
		if validLegacyJSONPath || validJSONPath {
			valJSON, err := json.Marshal(val)
			if err != nil {
				return fmt.Errorf("marshalling value: %w", err)
			}

			err = t.addColumnTypeAndValue(pi, prefix+key, valJSON, true, data, columnType)
			if err != nil {
				return fmt.Errorf("adding column type and value: %w", err)
			}
		} else if utils.IsObject(val) && (pi.event.Metadata.SourceCategory != "cloud" || level < 3) {
			err := t.setDataAndColumnTypeFromInput(pi, val.(map[string]any), data, columnType, completePrefix+key+"_", completeLevel+1, prefix+key+"_", level+1)
			if err != nil {
				return fmt.Errorf("setting data and column types from message: %w", err)
			}
		} else {
			tempData := val
			if pi.event.Metadata.SourceCategory == "cloud" && level >= 3 && utils.IsObject(val) {
				var err error
				tempData, err = json.Marshal(val)
				if err != nil {
					return fmt.Errorf("marshalling value: %w", err)
				}
			}
			err := t.addColumnTypeAndValue(pi, prefix+key, tempData, false, data, columnType)
			if err != nil {
				return fmt.Errorf("adding column type and value: %w", err)
			}
		}
	}
	return nil
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

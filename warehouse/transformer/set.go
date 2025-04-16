package transformer

import (
	"fmt"
	"strings"

	"github.com/samber/lo"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-server/jsonrs"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/rules"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/stringlikeobject"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
)

func setDataAndMetadataFromInput(
	tec *transformEventContext,
	input any,
	data map[string]any, metadata map[string]string,
	pi *prefixInfo,
) error {
	if input == nil || !utils.IsObject(input) {
		return nil
	}

	inputMap := input.(map[string]any)

	if len(inputMap) == 0 {
		return nil
	}
	if shouldHandleStringLikeObject(inputMap, pi) {
		return handleStringLikeObject(tec, inputMap, data, metadata, pi)
	}
	for _, key := range tec.sorter(lo.Keys(inputMap)) {
		val := inputMap[key]
		if utils.IsBlank(val) {
			continue
		}
		if isValidJSONPath(tec, key, pi) {
			if err := handleValidJSONPath(tec, key, val, data, metadata, pi); err != nil {
				return fmt.Errorf("handling valid JSON path: %w", err)
			}
		} else if shouldProcessNestedObject(tec, val, pi) {
			if err := processNestedObject(tec, key, val.(map[string]any), data, metadata, pi); err != nil {
				return fmt.Errorf("processing nested object: %w", err)
			}
		} else {
			if err := processNonNestedObject(tec, key, val, data, metadata, pi); err != nil {
				return fmt.Errorf("handling non-nested object: %w", err)
			}
		}
	}
	return nil
}

func shouldHandleStringLikeObject(inputMap map[string]any, pi *prefixInfo) bool {
	return (strings.HasSuffix(pi.completePrefix, "context_traits_") || pi.completePrefix == "group_traits_") && stringlikeobject.IsStringLikeObject(inputMap)
}

func handleStringLikeObject(
	tec *transformEventContext,
	inputMap map[string]any,
	data map[string]any, metadata map[string]string,
	pi *prefixInfo,
) error {
	if pi.prefix != "context_traits_" {
		return nil
	}
	err := addDataAndMetadata(tec, pi.prefix, stringlikeobject.ToString(inputMap), false, data, metadata)
	if err != nil {
		return fmt.Errorf("adding column type and value: %w", err)
	}
	return nil
}

func addDataAndMetadata(tec *transformEventContext, key string, val any, isJSONKey bool, data map[string]any, metadata map[string]string) error {
	columnName := transformColumnNameCached(tec, key)
	if len(columnName) == 0 {
		return nil
	}

	safeKey, err := safeColumnNameCached(tec, columnName)
	if err != nil {
		return fmt.Errorf("transforming column name: %w", err)
	}

	if rules.IsRudderReservedColumn(tec.event.Metadata.EventType, safeKey) {
		return nil
	}

	dataType := dataTypeFor(tec.event.Metadata.DestinationType, key, val, isJSONKey)
	metadata[safeKey] = dataType

	switch safeKey {
	case "context_tracking_plan_version", "CONTEXT_TRACKING_PLAN_VERSION":
		data[safeKey] = convertToFloat64IfInteger(val)
	case "context_violation_errors", "CONTEXT_VIOLATION_ERRORS":
		data[safeKey] = convertToSliceIfViolationErrors(val)
	default:
		data[safeKey] = convertValIfDateTime(val, dataType)
	}
	return nil
}

func isValidJSONPath(tec *transformEventContext, key string, pi *prefixInfo) bool {
	validLegacyJSONPath := isValidLegacyJSONPathKey(tec.event.Metadata.EventType, pi.prefix+key, pi.level, tec.jsonPathsInfo.legacyKeysMap)
	validJSONPath := isValidJSONPathKey(pi.completePrefix+key, pi.completeLevel, tec.jsonPathsInfo.keysMap)
	return validLegacyJSONPath || validJSONPath
}

func handleValidJSONPath(
	tec *transformEventContext,
	key string, val any,
	data map[string]any, metadata map[string]string,
	pi *prefixInfo,
) error {
	valJSON, err := jsonrs.Marshal(val)
	if err != nil {
		return fmt.Errorf("marshalling value: %w", err)
	}
	return addDataAndMetadata(tec, pi.prefix+key, string(valJSON), true, data, metadata)
}

func shouldProcessNestedObject(tec *transformEventContext, val any, pi *prefixInfo) bool {
	return utils.IsObject(val) && (tec.event.Metadata.SourceCategory != "cloud" || pi.level < 3)
}

func processNestedObject(
	tec *transformEventContext,
	key string, val map[string]any,
	data map[string]any, metadata map[string]string,
	pi *prefixInfo,
) error {
	newPrefixDetails := &prefixInfo{
		completePrefix: pi.completePrefix + key + "_",
		completeLevel:  pi.completeLevel + 1,
		prefix:         pi.prefix + key + "_",
		level:          pi.level + 1,
	}
	return setDataAndMetadataFromInput(tec, val, data, metadata, newPrefixDetails)
}

func processNonNestedObject(
	tec *transformEventContext,
	key string, val any,
	data map[string]any, metadata map[string]string,
	pi *prefixInfo,
) error {
	finalValue := val
	if tec.event.Metadata.SourceCategory == "cloud" && pi.level >= 3 && utils.IsObject(val) {
		jsonData, err := jsonrs.Marshal(val)
		if err != nil {
			return fmt.Errorf("marshalling value: %w", err)
		}
		finalValue = string(jsonData)
	}
	return addDataAndMetadata(tec, pi.prefix+key, finalValue, false, data, metadata)
}

func setDataAndMetadataFromRules(
	tec *transformEventContext,
	data map[string]any, metadata map[string]string,
	rules map[string]rules.Rules,
) error {
	for _, colKey := range tec.sorter(lo.Keys(rules)) {
		rule := rules[colKey]
		columnName, err := safeColumnNameCached(tec, colKey)
		if err != nil {
			return fmt.Errorf("safe column name: %w", err)
		}

		delete(data, columnName)
		delete(metadata, columnName)

		colVal, err := rule(tec.event)
		if err != nil {
			return fmt.Errorf("applying functional rule: %w", err)
		}
		if utils.IsBlank(colVal) || utils.IsObject(colVal) {
			continue
		}

		dataType := dataTypeFor(tec.event.Metadata.DestinationType, colKey, colVal, false)

		data[columnName] = convertValIfDateTime(colVal, dataType)
		metadata[columnName] = dataType
	}
	return nil
}

func (t *Transformer) storeRudderEvent(
	tec *transformEventContext,
	data map[string]any, metadata map[string]string,
) error {
	if !tec.destOpts.storeFullEvent {
		return nil
	}

	columnName, err := safeColumnNameCached(tec, "rudder_event")
	if err != nil {
		return fmt.Errorf("safe column name: %w", err)
	}

	eventJSON, err := jsonrs.Marshal(tec.event.Message)
	if err != nil {
		return fmt.Errorf("marshalling event: %w", err)
	}
	if t.config.populateSrcDestInfoInContext.Load() {
		eventJSON, err = sjson.SetBytes(eventJSON, "context.sourceId", tec.event.Metadata.SourceID)
		if err != nil {
			return fmt.Errorf("setting source id: %w", err)
		}
		eventJSON, err = sjson.SetBytes(eventJSON, "context.sourceType", tec.event.Metadata.SourceType)
		if err != nil {
			return fmt.Errorf("setting source type: %w", err)
		}
		eventJSON, err = sjson.SetBytes(eventJSON, "context.destinationId", tec.event.Metadata.DestinationID)
		if err != nil {
			return fmt.Errorf("setting destination id: %w", err)
		}
		eventJSON, err = sjson.SetBytes(eventJSON, "context.destinationType", tec.event.Metadata.DestinationType)
		if err != nil {
			return fmt.Errorf("setting destination type: %w", err)
		}
	}

	data[columnName] = string(eventJSON)
	metadata[columnName] = utils.GetFullEventColumnTypeByDestType(tec.event.Metadata.DestinationType)
	return nil
}

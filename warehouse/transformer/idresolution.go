package transformer

import (
	"fmt"
	"strings"

	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/response"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
)

func (t *Transformer) mergeEvents(tec *transformEventContext) ([]map[string]any, error) {
	if !t.config.enableIDResolution.Load() {
		return nil, nil
	}
	if !utils.IsIdentityEnabled(tec.event.Metadata.DestinationType) {
		return nil, nil
	}
	mergeProp1, mergeProp2, err := mergeProps(tec.event.Message, tec.event.Metadata)
	if err != nil {
		return nil, fmt.Errorf("merge: merge properties: %w", err)
	}
	if isMergePropEmpty(mergeProp1) {
		return nil, nil
	}

	tableName, err := mergeRuleTable(tec)
	if err != nil {
		return nil, fmt.Errorf("merge: merge rules table: %w", err)
	}
	columns, err := mergeRuleColumns(tec)
	if err != nil {
		return nil, fmt.Errorf("merge: merge columns: %w", err)
	}

	data := map[string]any{
		columns.Prop1Type:  utils.ToString(mergeProp1.Type),
		columns.Prop1Value: utils.ToString(mergeProp1.Value),
	}
	columnTypes := map[string]any{
		columns.Prop1Type:  model.StringDataType,
		columns.Prop1Value: model.StringDataType,
	}
	metadata := map[string]any{
		"table":        tableName,
		"columns":      columnTypes,
		"isMergeRule":  true,
		"receivedAt":   tec.event.Metadata.ReceivedAt,
		"mergePropOne": data[columns.Prop1Value],
	}

	if !isMergePropEmpty(mergeProp2) {
		data[columns.Prop2Type] = utils.ToString(mergeProp2.Type)
		data[columns.Prop2Value] = utils.ToString(mergeProp2.Value)
		columnTypes[columns.Prop2Type] = model.StringDataType
		columnTypes[columns.Prop2Value] = model.StringDataType

		metadata["mergePropTwo"] = data[columns.Prop2Value]
	}

	mergeOutput := map[string]any{
		"data":     data,
		"metadata": metadata,
		"userId":   "",
	}
	return []map[string]any{mergeOutput}, nil
}

func mergeProps(message types.SingularEventT, metadata ptrans.Metadata) (*mergeRule, *mergeRule, error) {
	switch strings.ToLower(metadata.EventType) {
	case "merge":
		return mergePropsForMergeEventType(message)
	case "alias":
		return mergePropsForAliasEventType(message)
	default:
		return mergePropsForDefaultEventType(message)
	}
}

func mergePropsForMergeEventType(message types.SingularEventT) (*mergeRule, *mergeRule, error) {
	mergeProperties := misc.MapLookup(message, "mergeProperties")
	if mergeProperties == nil {
		return nil, nil, response.ErrMergePropertiesMissing
	}
	mergePropertiesArr, ok := mergeProperties.([]any)
	if !ok {
		return nil, nil, response.ErrMergePropertiesNotArray
	}
	if len(mergePropertiesArr) != 2 {
		return nil, nil, response.ErrMergePropertiesNotSufficient
	}

	mergePropertiesMap0, ok := mergePropertiesArr[0].(map[string]any)
	if !ok {
		return nil, nil, response.ErrMergePropertyOneInvalid
	}
	mergePropertiesMap1, ok := mergePropertiesArr[1].(map[string]any)
	if !ok {
		return nil, nil, response.ErrMergePropertyTwoInvalid
	}

	mergeProperties0Type := misc.MapLookup(mergePropertiesMap0, "type")
	mergeProperties0Value := misc.MapLookup(mergePropertiesMap0, "value")
	mergeProperties1Type := misc.MapLookup(mergePropertiesMap1, "type")
	mergeProperties1Value := misc.MapLookup(mergePropertiesMap1, "value")

	if utils.IsBlank(mergeProperties0Type) || utils.IsBlank(mergeProperties0Value) || utils.IsBlank(mergeProperties1Type) || utils.IsBlank(mergeProperties1Value) {
		return nil, nil, response.ErrMergePropertyEmpty
	}

	mergeProp1 := &mergeRule{Type: mergeProperties0Type, Value: mergeProperties0Value}
	mergeProp2 := &mergeRule{Type: mergeProperties1Type, Value: mergeProperties1Value}
	return mergeProp1, mergeProp2, nil
}

func mergePropsForAliasEventType(message types.SingularEventT) (*mergeRule, *mergeRule, error) {
	userID := misc.MapLookup(message, "userId")
	previousID := misc.MapLookup(message, "previousId")

	mergeProp1 := &mergeRule{Type: "user_id", Value: userID}
	mergeProp2 := &mergeRule{Type: "user_id", Value: previousID}
	return mergeProp1, mergeProp2, nil
}

func mergePropsForDefaultEventType(message types.SingularEventT) (*mergeRule, *mergeRule, error) {
	anonymousID := misc.MapLookup(message, "anonymousId")
	userID := misc.MapLookup(message, "userId")

	var mergeProp1, mergeProp2 *mergeRule
	if utils.IsBlank(anonymousID) {
		mergeProp1 = &mergeRule{Type: "user_id", Value: userID}
	} else {
		mergeProp1 = &mergeRule{Type: "anonymous_id", Value: anonymousID}
		mergeProp2 = &mergeRule{Type: "user_id", Value: userID}
	}
	return mergeProp1, mergeProp2, nil
}

func mergeRuleTable(tec *transformEventContext) (string, error) {
	return safeTableNameCached(tec, "rudder_identity_merge_rules")
}

func mergeRuleColumns(tec *transformEventContext) (*mergeRulesColumns, error) {
	var (
		columns [4]string
		err     error
	)

	for i, col := range []string{
		"merge_property_1_type", "merge_property_1_value", "merge_property_2_type", "merge_property_2_value",
	} {
		if columns[i], err = safeColumnNameCached(tec, col); err != nil {
			return nil, fmt.Errorf("safe column name for %s: %w", col, err)
		}
	}

	rulesColumns := &mergeRulesColumns{
		Prop1Type: columns[0], Prop1Value: columns[1], Prop2Type: columns[2], Prop2Value: columns[3],
	}
	return rulesColumns, nil
}

func isMergePropEmpty(mergeProp *mergeRule) bool {
	return mergeProp == nil || utils.IsBlank(mergeProp.Type) || utils.IsBlank(mergeProp.Value)
}

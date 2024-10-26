package transformer

import (
	"fmt"
	"strings"

	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/datatype"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/response"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
)

type mergeRule struct {
	Type, Value any
}

type mergeRulesColumns struct {
	Prop1Type, Prop1Value, Prop2Type, Prop2Value string
}

func (t *transformer) handleMergeEvent(pi *processingInfo) ([]map[string]any, error) {
	if !t.config.enableIDResolution.Load() {
		return nil, nil
	}
	if !utils.IsIdentityEnabled(pi.event.Metadata.DestinationType) {
		return nil, nil
	}

	mergeProp1, mergeProp2, err := mergeProps(pi.event.Message, pi.event.Metadata)
	if err != nil {
		return nil, fmt.Errorf("merge: merge properties: %w", err)
	}
	if isMergePropEmpty(mergeProp1) {
		return nil, nil
	}

	tableName, err := mergeRuleTable(pi.event.Metadata.DestinationType, pi.itrOpts)
	if err != nil {
		return nil, fmt.Errorf("merge: merge rules table: %w", err)
	}
	columns, err := mergeRuleColumns(pi.event.Metadata.DestinationType, pi.itrOpts)
	if err != nil {
		return nil, fmt.Errorf("merge: merge columns: %w", err)
	}

	event := map[string]any{
		columns.Prop1Type:  utils.ToString(mergeProp1.Type),
		columns.Prop1Value: utils.ToString(mergeProp1.Value),
	}
	columnTypes := map[string]any{
		columns.Prop1Type:  datatype.TypeString,
		columns.Prop1Value: datatype.TypeString,
	}
	metadata := map[string]any{
		"table":        tableName,
		"columns":      columnTypes,
		"isMergeRule":  true,
		"receivedAt":   pi.event.Metadata.ReceivedAt,
		"mergePropOne": event[columns.Prop1Value],
	}

	if !isMergePropEmpty(mergeProp2) {
		event[columns.Prop2Type] = utils.ToString(mergeProp2.Type)
		event[columns.Prop2Value] = utils.ToString(mergeProp2.Value)
		columnTypes[columns.Prop2Type] = datatype.TypeString
		columnTypes[columns.Prop2Value] = datatype.TypeString

		metadata["mergePropTwo"] = event[columns.Prop2Value]
	}

	mergeOutput := map[string]any{
		"data":     event,
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

	if mergeProperties0Type == nil || mergeProperties0Value == nil || mergeProperties1Type == nil || mergeProperties1Value == nil {
		return nil, nil, response.ErrMergePropertyEmpty
	}
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
	if anonymousID == nil || utils.IsBlank(anonymousID) {
		mergeProp1 = &mergeRule{Type: "user_id", Value: userID}
	} else {
		mergeProp1 = &mergeRule{Type: "anonymous_id", Value: anonymousID}
		mergeProp2 = &mergeRule{Type: "user_id", Value: userID}
	}
	return mergeProp1, mergeProp2, nil
}

func mergeRuleTable(destType string, options integrationsOptions) (string, error) {
	return SafeTableName(destType, options, "rudder_identity_merge_rules")
}

func mergeRuleColumns(destType string, options integrationsOptions) (*mergeRulesColumns, error) {
	var (
		columns [4]string
		err     error
	)

	for i, col := range []string{
		"merge_property_1_type", "merge_property_1_value", "merge_property_2_type", "merge_property_2_value",
	} {
		if columns[i], err = SafeColumnName(destType, options, col); err != nil {
			return nil, fmt.Errorf("safe column name for %s: %w", col, err)
		}
	}

	rulesColumns := &mergeRulesColumns{
		Prop1Type: columns[0], Prop1Value: columns[1], Prop2Type: columns[2], Prop2Value: columns[3],
	}
	return rulesColumns, nil
}

func isMergePropEmpty(mergeProp *mergeRule) bool {
	return mergeProp == nil || mergeProp.Type == nil || mergeProp.Value == nil || utils.IsBlank(mergeProp.Type) || utils.IsBlank(mergeProp.Value)
}

package transformer

import (
	"fmt"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/rules"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
)

func (t *transformer) handleIdentifyEvent(pi *processingInfo) ([]map[string]any, error) {
	commonProps, commonColumnTypes, err := t.identifyCommonProps(pi)
	if err != nil {
		return nil, fmt.Errorf("identifies: common properties: %w", err)
	}

	identifiesResponse, err := t.identifiesResponse(pi, commonProps, commonColumnTypes)
	if err != nil {
		return nil, fmt.Errorf("identifies response: %w", err)
	}

	mergeEvents, err := t.handleMergeEvent(pi)
	if err != nil {
		return nil, fmt.Errorf("identifies: merge event: %w", err)
	}

	usersResponse, err := t.usersResponse(pi, commonProps, commonColumnTypes)
	if err != nil {
		return nil, fmt.Errorf("identifies: users response: %w", err)
	}
	return append(append(identifiesResponse, mergeEvents...), usersResponse...), nil
}

func (t *transformer) identifyCommonProps(pi *processingInfo) (map[string]any, map[string]string, error) {
	commonProps := make(map[string]any)
	commonColumnTypes := make(map[string]string)

	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["userProperties"], commonProps, commonColumnTypes, &prefixInfo{
		completePrefix: "identify_userProperties_",
		completeLevel:  2,
	}); err != nil {
		return nil, nil, fmt.Errorf("identify common props: setting data and column types from message: %w", err)
	}
	if pi.dstOpts.allowUsersContextTraits {
		contextTraits := misc.MapLookup(pi.event.Message, "context", "traits")

		if err := t.setDataAndColumnTypeFromInput(pi, contextTraits, commonProps, commonColumnTypes, &prefixInfo{
			completePrefix: "identify_context_traits_",
			completeLevel:  3,
		}); err != nil {
			return nil, nil, fmt.Errorf("identify common props: setting data and column types from message: %w", err)
		}
	}
	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["traits"], commonProps, commonColumnTypes, &prefixInfo{
		completePrefix: "identify_traits_",
		completeLevel:  2,
	}); err != nil {
		return nil, nil, fmt.Errorf("identify common props: setting data and column types from message: %w", err)
	}
	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["context"], commonProps, commonColumnTypes, &prefixInfo{
		completePrefix: "identify_context_",
		completeLevel:  2,
		prefix:         "context_",
	}); err != nil {
		return nil, nil, fmt.Errorf("identify common props: setting data and column types from message: %w", err)
	}

	userIDColName, err := SafeColumnName(pi.event.Metadata.DestinationType, pi.itrOpts, "user_id")
	if err != nil {
		return nil, nil, fmt.Errorf("identify common props: safe column name: %w", err)
	}
	if k, ok := commonProps[userIDColName]; ok && k != nil {
		revUserIDCol := "_" + userIDColName

		commonProps[revUserIDCol] = commonProps[userIDColName]
		commonColumnTypes[revUserIDCol] = commonColumnTypes[userIDColName]

		delete(commonProps, userIDColName)
		delete(commonColumnTypes, userIDColName)
	}
	return commonProps, commonColumnTypes, nil
}

func (t *transformer) identifiesResponse(pi *processingInfo, commonProps map[string]any, commonColumnTypes map[string]string) ([]map[string]any, error) {
	event := make(map[string]any)
	columnTypes := make(map[string]string)

	event = lo.Assign(event, commonProps)

	if err := t.setDataAndColumnTypeFromRules(pi, event, columnTypes,
		rules.DefaultRules, rules.DefaultFunctionalRules,
	); err != nil {
		return nil, fmt.Errorf("identifies response: setting data and column types from rules: %w", err)
	}
	if err := storeRudderEvent(pi, event, columnTypes); err != nil {
		return nil, fmt.Errorf("identifies response: storing rudder event: %w", err)
	}

	identifiesTable, err := SafeTableName(pi.event.Metadata.DestinationType, pi.itrOpts, "identifies")
	if err != nil {
		return nil, fmt.Errorf("identifies response: safe table name: %w", err)
	}
	identifiesColumns, err := t.getColumns(pi.event.Metadata.DestinationType, event, lo.Assign(commonColumnTypes, columnTypes))
	if err != nil {
		return nil, fmt.Errorf("identifies response: getting columns: %w", err)
	}

	identifiesOutput := map[string]any{
		"data": event,
		"metadata": map[string]any{
			"table":      identifiesTable,
			"columns":    identifiesColumns,
			"receivedAt": pi.event.Metadata.ReceivedAt,
		},
		"userId": "",
	}
	return []map[string]any{identifiesOutput}, nil
}

func (t *transformer) usersResponse(pi *processingInfo, commonProps map[string]any, commonColumnTypes map[string]string) ([]map[string]any, error) {
	userID := misc.MapLookup(pi.event.Message, "userId")
	if userID == nil || utils.IsBlank(userID) {
		return nil, nil
	}
	if pi.itrOpts.skipUsersTable || pi.dstOpts.skipUsersTable {
		return nil, nil
	}

	event := make(map[string]any)
	columnTypes := make(map[string]string)

	event = lo.Assign(event, commonProps)

	var rulesMap map[string]string
	if utils.IsDataLake(pi.event.Metadata.DestinationType) {
		rulesMap = rules.IdentifyDataLakeRules
	} else {
		rulesMap = rules.IdentifyNonDataLakeRules
	}

	if err := t.setDataAndColumnTypeFromRules(pi, event, columnTypes,
		rulesMap, rules.IdentifyFunctionalRules,
	); err != nil {
		return nil, fmt.Errorf("users response: setting data and column types from rules: %w", err)
	}

	idColName, err := SafeColumnName(pi.event.Metadata.DestinationType, pi.itrOpts, "id")
	if err != nil {
		return nil, fmt.Errorf("users response: safe column name: %w", err)
	}
	event[idColName] = userID
	columnTypes[idColName] = t.getDataType(pi.event.Metadata.DestinationType, idColName, userID, false)

	receivedAtColName, err := SafeColumnName(pi.event.Metadata.DestinationType, pi.itrOpts, "received_at")
	if err != nil {
		return nil, fmt.Errorf("users response: safe column name: %w", err)
	}
	event[receivedAtColName] = pi.event.Metadata.ReceivedAt
	columnTypes[receivedAtColName] = "datetime"

	tableName, err := SafeTableName(pi.event.Metadata.DestinationType, pi.itrOpts, "users")
	if err != nil {
		return nil, fmt.Errorf("users response: safe table name: %w", err)
	}
	columns, err := t.getColumns(pi.event.Metadata.DestinationType, event, lo.Assign(commonColumnTypes, columnTypes))
	if err != nil {
		return nil, fmt.Errorf("users response: getting columns: %w", err)
	}

	usersOutput := map[string]any{
		"data": event,
		"metadata": map[string]any{
			"table":      tableName,
			"columns":    columns,
			"receivedAt": pi.event.Metadata.ReceivedAt,
		},
		"userId": "",
	}
	return []map[string]any{usersOutput}, nil
}

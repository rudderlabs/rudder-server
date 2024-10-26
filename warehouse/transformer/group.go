package transformer

import (
	"fmt"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/rules"
)

func (t *transformer) handleGroupEvent(pi *processingInfo) ([]map[string]any, error) {
	event := make(map[string]any)
	columnTypes := make(map[string]string)

	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["traits"], event, columnTypes, &prefixInfo{
		completePrefix: "group_traits_",
		completeLevel:  2,
	}); err != nil {
		return nil, fmt.Errorf("group: setting data and column types from input: %w", err)
	}
	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["context"], event, columnTypes, &prefixInfo{
		completePrefix: "group_context_",
		completeLevel:  2,
		prefix:         "context_",
	}); err != nil {
		return nil, fmt.Errorf("group: setting data and column types from input: %w", err)
	}
	if err := t.setDataAndColumnTypeFromRules(pi, event, columnTypes,
		lo.Assign(rules.DefaultRules, rules.GroupRules), rules.DefaultFunctionalRules,
	); err != nil {
		return nil, fmt.Errorf("group: setting data and column types from rules: %w", err)
	}

	if err := storeRudderEvent(pi, event, columnTypes); err != nil {
		return nil, fmt.Errorf("group: storing rudder event: %w", err)
	}

	tableName, err := SafeTableName(pi.event.Metadata.DestinationType, pi.itrOpts, "groups")
	if err != nil {
		return nil, fmt.Errorf("group: safe table name: %w", err)
	}
	columns, err := t.getColumns(pi.event.Metadata.DestinationType, event, columnTypes)
	if err != nil {
		return nil, fmt.Errorf("group: getting columns: %w", err)
	}

	mergeEvents, err := t.handleMergeEvent(pi)
	if err != nil {
		return nil, fmt.Errorf("group: merge event: %w", err)
	}

	groupOutput := map[string]any{
		"data": event,
		"metadata": map[string]any{
			"table":      tableName,
			"columns":    columns,
			"receivedAt": pi.event.Metadata.ReceivedAt,
		},
		"userId": "",
	}
	return append([]map[string]any{groupOutput}, mergeEvents...), nil
}

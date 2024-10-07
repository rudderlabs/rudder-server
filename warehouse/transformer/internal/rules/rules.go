package rules

import (
	"strings"

	"github.com/samber/lo"

	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
)

type FunctionalRules func(event ptrans.TransformerEvent) (any, error)

var rudderReservedColumns = map[string]map[string]struct{}{
	"track":    createReservedColumns(lo.Keys(DefaultRules, TrackRules), lo.Keys(DefaultFunctionalRules, TrackTableFunctionalRules, TrackEventTableFunctionalRules)),
	"page":     createReservedColumns(lo.Keys(DefaultRules), lo.Keys(DefaultFunctionalRules, PageFunctionalRules)),
	"screen":   createReservedColumns(lo.Keys(DefaultRules), lo.Keys(DefaultFunctionalRules, ScreenFunctionalRules)),
	"identify": createReservedColumns(lo.Keys(DefaultRules, IdentifyDataLakeRules, IdentifyNonDataLakeRules), lo.Keys(DefaultFunctionalRules, IdentifyFunctionalRules)),
	"group":    createReservedColumns(lo.Keys(DefaultRules, GroupRules), lo.Keys(DefaultFunctionalRules)),
	"alias":    createReservedColumns(lo.Keys(DefaultRules, AliasRules), lo.Keys(DefaultFunctionalRules)),
	"extract":  createReservedColumns(lo.Keys(ExtractRules), lo.Keys(ExtractFunctionalRules)),
}

func firstValidValue(message map[string]any, props []string) any {
	for _, prop := range props {
		propKeys := strings.Split(prop, ".")
		if val := misc.MapLookup(message, propKeys...); val != nil && !utils.IsBlank(val) {
			return val
		}
	}
	return nil
}

func createReservedColumns(rules, functionRules []string) map[string]struct{} {
	return lo.SliceToMap(append(lo.Uniq(rules), lo.Uniq(functionRules)...), func(item string) (string, struct{}) {
		return item, struct{}{}
	})
}

func IsRudderReservedColumn(eventType, columnName string) bool {
	if _, ok := rudderReservedColumns[strings.ToLower(eventType)]; !ok {
		return false
	}
	if _, ok := rudderReservedColumns[strings.ToLower(eventType)][strings.ToLower(columnName)]; ok {
		return true
	}
	return false
}

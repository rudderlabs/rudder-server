package rules

import (
	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
)

var PageFunctionalRules = map[string]FunctionalRules{
	"name": func(event ptrans.TransformerEvent) (any, error) {
		return firstValidValue(event.Message, []string{"name", "properties.name"}), nil
	},
}

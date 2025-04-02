package integrations

import (
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jsonrs"
	"github.com/rudderlabs/rudder-server/processor/types"
)

// PostParametersT is a struct for holding all the values from transformerResponse and use them to publish an event to a destination
// optional is a custom tag introduced by us and is handled by GetMandatoryJSONFieldNames. Its intentionally added
// after two commas because the tag that comes after the first comma should be known by json parser
type PostParametersT struct {
	Type          string `json:"type"`
	URL           string `json:"endpoint"`
	RequestMethod string `json:"method"`
	// Invalid tag used in struct. skipcq: SCC-SA5008
	UserID      string                 `json:"userId,,optional"` //nolint:staticcheck
	Headers     map[string]interface{} `json:"headers"`
	QueryParams map[string]interface{} `json:"params"`
	Body        map[string]interface{} `json:"body"`
	Files       map[string]interface{} `json:"files"`
}

type TransStatsT struct {
	StatTags map[string]string `json:"statTags"`
}

func CollectDestErrorStats(input []byte) {
	var integrationStat TransStatsT
	err := jsonrs.Unmarshal(input, &integrationStat)
	if err == nil {
		if len(integrationStat.StatTags) > 0 {
			stats.Default.NewTaggedStat("integration.failure_detailed", stats.CountType, integrationStat.StatTags).Increment()
		}
	}
}

func CollectIntgTransformErrorStats(input []byte) {
	var integrationStats []TransStatsT
	err := jsonrs.Unmarshal(input, &integrationStats)
	if err == nil {
		for _, integrationStat := range integrationStats {
			if len(integrationStat.StatTags) > 0 {
				stats.Default.NewTaggedStat("integration.failure_detailed", stats.CountType, integrationStat.StatTags).Increment()
			}
		}
	}
}

// FilterClientIntegrations parses the destination names from the
// input JSON, matches them with enabled destinations from controle plane and returns the IDSs
func FilterClientIntegrations(clientEvent types.SingularEventT, destNameIDMap map[string]backendconfig.DestinationDefinitionT) (retVal []string) {
	clientIntgs, ok := types.GetRudderEventVal("integrations", clientEvent)
	if !ok {
		clientIntgs = make(map[string]interface{})
	}
	clientIntgsList, ok := clientIntgs.(map[string]interface{})
	if !ok {
		return
	}
	// All is by default true, if not present make it true
	allVal, found := clientIntgsList["All"]
	if !found {
		allVal = true
	}
	_, isAllBoolean := allVal.(bool)
	if !isAllBoolean {
		return
	}
	var outVal []string
	for dest := range destNameIDMap {
		_, isBoolean := clientIntgsList[dest].(bool)
		// if dest is bool and is present in clientIntgretaion list, check if true/false
		if isBoolean {
			if clientIntgsList[dest] == true {
				outVal = append(outVal, destNameIDMap[dest].Name)
			}
			continue
		}
		// Always add for syntax dest:{...}
		_, isMap := clientIntgsList[dest].(map[string]interface{})
		if isMap {
			outVal = append(outVal, destNameIDMap[dest].Name)
			continue
		}
		// if dest  not present in clientIntgretaion list, add based on All flag
		if allVal.(bool) {
			outVal = append(outVal, destNameIDMap[dest].Name)
		}
	}
	retVal = outVal
	return
}

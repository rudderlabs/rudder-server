package backendconfig

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// admin is container object to expose admin functions
type admin struct{}

// RoutingConfig reports current backend config and process config after masking secret fields
func (*admin) RoutingConfig(filterProcessor bool, reply *string) (err error) { // skipcq: RVV-A0005
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()

	outputJSON := getConfig()
	if filterProcessor {
		outputJSON = filterProcessorEnabledWorkspaceConfig(outputJSON)
	}

	outputObj := make(map[string]interface{}, len(outputJSON))

	for workspaceID, wConfig := range outputJSON {
		wSources := make([]interface{}, 0, len(wConfig.Sources))
		for _, source := range wConfig.Sources {
			destinations := make([]interface{}, 0)
			for _, destination := range source.Destinations { // TODO skipcq: CRT-P0006
				destinationConfigCopy := make(map[string]interface{})
				for k, v := range destination.Config {
					destinationConfigCopy[k] = v
				}

				// Mask secret config fields by replacing latter 2/3rd of the field with 'x's
				destinationSecretKeys, ok := destination.DestinationDefinition.Config["secretKeys"]
				if !ok {
					destinationSecretKeys = []interface{}{}
				}

				rt := reflect.TypeOf(destinationSecretKeys)
				switch rt.Kind() {
				case reflect.Array:
				case reflect.Slice:
				default:
					return fmt.Errorf("secretKeys field of destination definition config is not an array. Destination definition name: %s", destination.DestinationDefinition.DisplayName)
				}

				for _, k := range destinationSecretKeys.([]interface{}) {
					secretKey := k.(string)
					secret, ok := destinationConfigCopy[secretKey]
					if ok {
						switch v := secret.(type) {
						case string:
							s := secret.(string)
							mask := strings.Repeat("x", (len(s)*2)/3)
							destinationConfigCopy[secretKey] = s[0:len(s)-len(mask)] + mask
						default:
							return fmt.Errorf("I don't know how to handle secret config field of type %T", v)
						}
					}
				}

				destinations = append(destinations, map[string]interface{}{
					"name":              destination.Name,
					"enabled":           destination.Enabled,
					"processor-enabled": destination.IsProcessorEnabled,
					"id":                destination.ID,
					"config":            destinationConfigCopy,
					"type":              destination.DestinationDefinition.DisplayName,
				})
			}
			wSources = append(wSources, map[string]interface{}{
				"name":         source.Name,
				"config":       source.Config,
				"writekey":     source.WriteKey,
				"id":           source.ID,
				"enabled":      source.Enabled,
				"destinations": destinations,
				"type":         source.SourceDefinition.Name,
			})
		}
		outputObj[workspaceID] = wSources
	}

	formattedOutput, err := json.MarshalIndent(outputObj, "", "  ")
	*reply = string(formattedOutput)
	return err
}

package backendconfig

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// BackendConfigAdmin is container object to expose admin functions
type BackendConfigAdmin struct{}

// RoutingConfig reports current backend config and process config after masking secret fields
func (bca *BackendConfigAdmin) RoutingConfig(filterProcessor bool, reply *string) (err error) {

	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("Internal Rudder Server Error. Error: %v", r)
		}
	}()

	curSourceJSONLock.RLock()
	defer curSourceJSONLock.RUnlock()
	outputJSON := curSourceJSON
	if filterProcessor {
		outputJSON = filterProcessorEnabledDestinations(outputJSON)
	}

	outputObj := make([]interface{}, 0)

	for _, source := range outputJSON.Sources {
		destinations := make([]interface{}, 0)
		for _, destination := range source.Destinations {
			destinationConfigCopy := make(map[string]interface{})
			for k, v := range destination.Config {
				destinationConfigCopy[k] = v
			}

			//Mask secret config fields by replacing latter 2/3rd of the field with 'x's
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
		outputObj = append(outputObj, map[string]interface{}{
			"name":         source.Name,
			"config":       source.Config,
			"writekey":     source.WriteKey,
			"id":           source.ID,
			"enabled":      source.Enabled,
			"destinations": destinations,
			"type":         source.SourceDefinition.Name,
		})
	}

	formattedOutput, err := json.MarshalIndent(outputObj, "", "  ")
	*reply = string(formattedOutput)
	return err
}

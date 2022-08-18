package transformer

import (
	"errors"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

// GetSupportedMessageTypes returns the supported message types for the given event, based on configuration.
// If no relevant configuration is found, returns an error
func (event *TransformerEventT) GetSupportedMessageTypes() ([]string, error) {
	var supportedMessageTypes []string
	if supportedTypes, ok := event.Destination.DestinationDefinition.Config["supportedMessageTypes"]; ok {
		if supportedTypeInterface, ok := supportedTypes.([]interface{}); ok {
			supportedTypesArr := misc.ConvertInterfaceToStringArray(supportedTypeInterface)
			for _, supportedType := range supportedTypesArr {
				var skip bool
				switch supportedType {
				case "identify":
					skip = event.identifyDisabled()
				default:
				}
				if !skip {
					supportedMessageTypes = append(supportedMessageTypes, supportedType)
				}
			}
			return supportedMessageTypes, nil
		}
		return supportedMessageTypes, errors.New("supportedMessageTypes not an array")
	}
	return supportedMessageTypes, errors.New("no supportedMessageTypes found")
}

func (event *TransformerEventT) identifyDisabled() bool {
	if serverSideIdentify, flag := event.Destination.Config["enableServerSideIdentify"]; flag {
		if v, ok := serverSideIdentify.(bool); ok {
			return !v
		}
	}
	return false
}

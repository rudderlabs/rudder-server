package eventfilter

import (
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// GetSupportedMessageTypes returns the supported message types for the given event, based on configuration.
// If no relevant configuration is found, returns false
func GetSupportedMessageTypes(destination *backendconfig.DestinationT) ([]string, bool) {
	var supportedMessageTypes []string
	if supportedTypes, ok := destination.DestinationDefinition.Config["supportedMessageTypes"]; ok {
		if supportedTypeInterface, ok := supportedTypes.([]interface{}); ok {
			supportedTypesArr := misc.ConvertInterfaceToStringArray(supportedTypeInterface)
			for _, supportedType := range supportedTypesArr {
				var skip bool
				switch supportedType {
				case "identify":
					skip = identifyDisabled(destination)
				default:
				}
				if !skip {
					supportedMessageTypes = append(supportedMessageTypes, supportedType)
				}
			}
			return supportedMessageTypes, true
		}
	}
	return nil, false
}

func identifyDisabled(destination *backendconfig.DestinationT) bool {
	if serverSideIdentify, flag := destination.Config["enableServerSideIdentify"]; flag {
		if v, ok := serverSideIdentify.(bool); ok {
			return !v
		}
	}
	return false
}

// GetSupportedEvents returns the supported message events for the given destination, based on configuration.
// If no relevant configuration is found, returns false
func GetSupportedMessageEvents(destination *backendconfig.DestinationT) ([]string, bool) {
	// "listOfConversions": [
	//  	{
	//  		"conversions": "Credit Card Added"
	//  	},
	//  	{
	//  		"conversions": "Credit Card Removed"
	//	    }
	// ]
	if supportedEventsI, ok := destination.Config["listOfConversions"]; ok {
		if supportedEvents, ok := supportedEventsI.([]interface{}); ok {
			var supportedMessageEvents []string
			for _, supportedEvent := range supportedEvents {
				if supportedEventMap, ok := supportedEvent.(map[string]interface{}); ok {
					if conversions, ok := supportedEventMap["conversions"]; ok {
						if supportedMessageEvent, ok := conversions.(string); ok {
							supportedMessageEvents = append(supportedMessageEvents, supportedMessageEvent)
						}
					}
				}
			}
			if len(supportedMessageEvents) != len(supportedEvents) {
				return nil, false
			}
			return supportedMessageEvents, true
		}
	}
	return nil, false
}

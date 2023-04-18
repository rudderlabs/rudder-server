package eventfilter

import (
	"strings"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/samber/lo"
)

var pkgLogger = logger.NewLogger().Child("eventfilter")

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

type EventFilterParams struct {
	Destination      *backendconfig.DestinationT
	SrcType          string
	Event            *types.SingularEventT
	DefaultBehaviour bool
}

func getEventType(event *types.SingularEventT) string {
	// Event-Type default check
	eventTypeI := misc.MapLookup(*event, "type")
	if eventTypeI == nil {
		pkgLogger.Error("Event type is not being sent for the event")
		// We will allow the event to be sent to destination transformation
		return ""
	}
	eventType, isEventTypeString := eventTypeI.(string)
	if !isEventTypeString {
		// Seems like it makes sense!
		pkgLogger.Errorf("Invalid event type :%v, Type assertion failed", eventTypeI)
		return ""
	}
	return eventType
}

/*
Lets the caller know if the event is allowed to flow through server for a `specific destination`
Introduced to support hybrid-mode, cloud-mode in a more scalable way

The template inside `destinationDefinition.Config.supportedConnectionModes` would look like this
```

	[sourceType]: {
		[connectionMode]: {
			[eventProperty]: [...supportedEventPropertyValues]
		}
	}

```

Example:

	{
		...
		"supportedConnectionModes": {
			"unity": {
				"cloud": {
					"messageTypes": { "allowAll": true },
				},
				"hybrid": {
					"messageTypes": { "allowAll": true },
				}
			},
			"web": {
				"cloud": {
					"messageTypes": { "allowAll": true },
				},
				"device": {
					"messageTypes": { "allowAll": true },
				}
			}
		},
		...
	}
*/
func FilterUsingSupportedConnectionModes(eventFilterParams EventFilterParams) bool {
	destination := eventFilterParams.Destination
	srcType := eventFilterParams.SrcType
	event := eventFilterParams.Event
	evaluatedDefaultBehaviour := eventFilterParams.DefaultBehaviour

	// Event-Type
	eventType := getEventType(event)
	if eventType == "" {
		// we might need to drop the event
		return false
	}

	eventType = strings.TrimSpace(strings.ToLower(eventType))

	/*
		```
		From /workspaceConfig, /data-planes/v1/namespaces/:namespace/config, we get
		{
			connectionMode: string
		}
	*/
	destConnModeI := misc.MapLookup(destination.Config, "connectionMode")
	if destConnModeI == nil {
		return evaluatedDefaultBehaviour
	}
	destConnectionMode, isDestConnModeString := destConnModeI.(string)
	if !isDestConnModeString || destConnectionMode == "device" {
		// includes Case 6
		pkgLogger.Errorf("Provided connectionMode is in wrong format or the mode is device", destConnModeI)
		return false
	}
	/*
		Cases:
		1. if supportedConnectionModes is not present -- rely on default behaviour
		2. if sourceType not in supportedConnectionModes && sourceType is in supportedSourceTypes (the sourceType is not defined) -- rely on default behaviour
		3. if sourceType = {} -- don't send the event(silently drop the event)
		4. if connectionMode not in supportedConnectionModes.sourceType (some connectionMode is not defined) -- don't send the event(silently drop the event)
		5. if sourceType.connectionMode = {} -- rely on default behaviour
		6. "device" is defined as a key for a sourceType -- don't send the event
		7. if eventProperty not in sourceType.connectionMode -- rely on property based default behaviour
		8. if sourceType.connectionMode.eventProperty = {} (both allowAll, allowedValues are not included in defConfig) -- rely on default behaviour
		9. if sourceType.connectionMode.eventProperty = { allowAll: true, allowedValues: ["track", "page"]} (both allowAll, allowedValues are included in defConfig) -- rely on default behaviour
	*/

	supportedConnectionModesI, connModesOk := destination.DestinationDefinition.Config["supportedConnectionModes"]
	if !connModesOk {
		// Probably the "supportedConnectionModes" key is not present, so we rely on Default behaviour
		return evaluatedDefaultBehaviour
	}
	supportedConnectionModes := supportedConnectionModesI.(map[string]interface{})

	supportedEventPropsMapI, supportedEventPropsLookupErr := misc.NestedMapLookup(supportedConnectionModes, srcType, destConnectionMode)
	if supportedEventPropsLookupErr != nil {
		if supportedEventPropsLookupErr.Level == 0 {
			// Case 2
			pkgLogger.Infof("Failed with %v for SourceType(%v) while looking up for it in supportedConnectionModes", supportedEventPropsLookupErr.Err.Error(), srcType)
			return evaluatedDefaultBehaviour
		}
		// Cases 3 & 4
		pkgLogger.Infof("Failed with %v for ConnectionMode(%v) while looking up for it in supportedConnectionModes", supportedEventPropsLookupErr.Err.Error(), destConnectionMode)
		return false
	}

	supportedEventPropsMap, isEventPropsICastableToMap := supportedEventPropsMapI.(map[string]interface{})
	if !isEventPropsICastableToMap || len(supportedEventPropsMap) == 0 {
		// includes Case 5, 7
		return evaluatedDefaultBehaviour
	}
	// Flag indicating to let the event pass through
	allowEvent := evaluatedDefaultBehaviour
	for eventProperty, supportedEventVals := range supportedEventPropsMap {
		eventPropMap, isMap := supportedEventVals.(map[string]interface{})

		if !allowEvent || !isMap {
			allowEvent = evaluatedDefaultBehaviour
			break
		}
		if eventProperty == "messageType" {
			eventPropMap := ConvertEventPropMapToStruct[string](eventPropMap)
			// eventPropMap == nil  -- occurs when both allowAll and allowedVals are not defined or when allowAll contains any value other than boolean
			// eventPropMap.AllowAll -- When only allowAll is defined
			//  len(eventPropMap.AllowedVals) == 0 -- when allowedVals is empty array(occurs when it is [] or when data-type of one of the values is not string)
			if eventPropMap == nil || eventPropMap.AllowAll || len(eventPropMap.AllowedVals) == 0 {
				allowEvent = evaluatedDefaultBehaviour
				continue
			}

			// when allowedValues is defined and non-empty array values
			pkgLogger.Debugf("SupportedVals: %v -- EventType from event: %v\n", eventPropMap.AllowedVals, eventType)
			allowEvent = lo.Contains(eventPropMap.AllowedVals, eventType) && evaluatedDefaultBehaviour
		}
	}
	return allowEvent
}

type EventPropsTypes interface {
	~string
}

/*
* Converts interface{} to []T if the go type-assertion allows it
 */
func ConvertToArrayOfType[T EventPropsTypes](data interface{}) []T {
	switch value := data.(type) {
	case []T:
		return value
	case []interface{}:
		result := make([]T, len(value))
		for i, v := range value {
			var ok bool
			result[i], ok = v.(T)
			if !ok {
				return []T{}
			}
		}
		return result
	}
	return []T{}
}

type EventProperty[T EventPropsTypes] struct {
	AllowAll    bool
	AllowedVals []T
}

/*
Converts the eventPropertyMap to a struct
In an eventPropertyMap we expect only one of [allowAll, allowedValues] properties

Possible cases

 1. { [eventProperty]: { allowAll: true/false }  }

    - One of the expected ways

    - EventProperty{AllowAll: true/false}

 2. { [eventProperty]: { allowedValues: [val1, val2, val3] }  }

    - One of the expected ways

    - Output: EventProperty{AllowedValues: [val1, val2, val3]}

 3. { [eventProperty]: { allowedValues: [val1, val2, val3], allowAll: true/false }  }

    - Not expected

    - EventProperty{AllowAll: true/false}

 4. { [eventProperty]: { }  }

    - Not expected

    - EventProperty{AllowAll:false}
*/
func ConvertEventPropMapToStruct[T EventPropsTypes](eventPropMap map[string]interface{}) *EventProperty[T] {
	var eventPropertyStruct EventProperty[T]
	for _, key := range []string{"allowAll", "allowedValues"} {
		val, ok := eventPropMap[key]
		if !ok {
			pkgLogger.Debugf("'%v' not found in eventPropertiesMap(supportedConnectionModes.sourceType.connectionMode.[eventProperty])", key)
			continue
		}
		switch key {
		case "allowAll":
			allowAll, convertable := val.(bool)
			if !convertable {
				return nil
			}
			eventPropertyStruct.AllowAll = allowAll
		case "allowedValues":
			allowedVals := ConvertToArrayOfType[T](val)
			eventPropertyStruct.AllowedVals = allowedVals
		}
	}
	return &eventPropertyStruct
}

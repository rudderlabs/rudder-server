package eventfilter

import (
	"strings"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/samber/lo"
)

const (
	eventProperty_allowAll      = "allowAll"
	eventProperty_allowedValues = "allowedValues"
)

var eventPropertyKeys = []string{eventProperty_allowAll, eventProperty_allowedValues}
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

type AllowTransformerEventParams struct {
	TransformerEvent      *transformer.TransformerEventT
	SupportedMessageTypes []string
}

type EventParams struct {
	MessageType string
}

type ConnectionModeFilterParams struct {
	Destination      *backendconfig.DestinationT
	SrcType          string
	Event            *EventParams
	DefaultBehaviour bool
}

func getMessageType(event *types.SingularEventT) string {
	eventMessageTypeI := misc.MapLookup(*event, "type")
	if eventMessageTypeI == nil {
		pkgLogger.Error("Message type is not being sent for the event")
		return ""
	}
	eventMessageType, isEventTypeString := eventMessageTypeI.(string)
	if !isEventTypeString {
		pkgLogger.Errorf("Invalid message type: type assertion failed: %v", eventMessageTypeI)
		return ""
	}
	return eventMessageType
}

/*
AllowEventToDestTransformation lets the caller know if we need to allow the event to proceed to destination transformation.

Currently this method supports below validations(executed in the same order):

1. Validate if messageType sent in event is included in SupportedMessageTypes

2. Validate if the event is sendable to destination based on connectionMode, sourceType & messageType
*/
func AllowEventToDestTransformation(transformerEvent *transformer.TransformerEventT, supportedMsgTypes []string) (bool, *transformer.TransformerResponseT) {
	// MessageType filtering -- STARTS
	messageType := strings.TrimSpace(strings.ToLower(getMessageType(&transformerEvent.Message)))
	if messageType == "" {
		// We will abort the event
		errMessage := "Invalid message type. Type assertion failed"
		resp := &transformer.TransformerResponseT{
			Output: transformerEvent.Message, StatusCode: 400,
			Metadata: transformerEvent.Metadata,
			Error:    errMessage,
		}
		return false, resp
	}

	isSupportedMsgType := lo.Contains(supportedMsgTypes, messageType)
	if !isSupportedMsgType {
		pkgLogger.Debug("event filtered out due to unsupported msg types")
		// We will not allow the event
		return false, nil
	}
	// MessageType filtering -- ENDS

	// srcType.connectionMode.[eventProperty] filtering -- STARTS
	allow, failedResponse := FilterUsingSupportedConnectionModes(ConnectionModeFilterParams{
		Destination: &transformerEvent.Destination,
		SrcType:     transformerEvent.Metadata.SourceDefinitionType,
		Event:       &EventParams{MessageType: messageType},
		// Default behavior
		// When something is missing in "supportedConnectionModes" or if "supportedConnectionModes" is not defined
		// We would be checking for below things
		// 1. Check if the event.type value is present in destination.DestinationDefinition.Config["supportedMessageTypes"]
		// 2. Check if the connectionMode of destination is cloud or hybrid(evaluated through `IsProcessorEnabled`)
		// Only when 1 & 2 are true, we would allow the event to flow through to server
		// As when this will be called, we would have already checked if event.type in supportedMessageTypes
		DefaultBehaviour: transformerEvent.Destination.IsProcessorEnabled && isSupportedMsgType,
	})

	if !allow {
		return allow, failedResponse
	}
	// srcType.connectionMode.[eventProperty] filtering -- ENDS

	return true, nil
}

/*
FilterUsingSupportedConnectionModes lets the caller know if the event is allowed to flow through server for a `specific destination`
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
					"messageType": { "allowAll": true },
				},
				"hybrid": {
					"messageType": { "allowedValues": ["group","track"] },
				}
			},
			"web": {
				"cloud": {
					"messageType": { "allowedValues": ["group","track"] },
				},
				"device": {
					"messageType": { "allowAll": true },
				}
			}
		},
		...
	}
*/
func FilterUsingSupportedConnectionModes(connectionModeFilterParams ConnectionModeFilterParams) (bool, *transformer.TransformerResponseT) {
	destination := connectionModeFilterParams.Destination
	srcType := connectionModeFilterParams.SrcType
	messageType := connectionModeFilterParams.Event.MessageType
	evaluatedDefaultBehaviour := connectionModeFilterParams.DefaultBehaviour

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

	destConnModeI := misc.MapLookup(destination.Config, "connectionMode")
	if destConnModeI == nil {
		pkgLogger.Debug("connectionMode not present, filtering event based on default behaviour")
		return evaluatedDefaultBehaviour, nil
	}
	destConnectionMode, isDestConnModeString := destConnModeI.(string)
	if !isDestConnModeString || destConnectionMode == "device" {
		// includes Case 6
		pkgLogger.Errorf("Provided connectionMode(%v) is in wrong format or the mode is device", destConnModeI)
		return false, nil
	}

	eventProperties := getSupportedEventPropertiesMap(destination.DestinationDefinition.Config, srcType, destConnectionMode, evaluatedDefaultBehaviour)
	if len(eventProperties.EventPropsMap) == 0 {
		return eventProperties.Allow, eventProperties.FailedResponse
	}

	// Flag indicating to let the event pass through
	allowEvent := evaluatedDefaultBehaviour
	for eventProperty, supportedEventVals := range eventProperties.EventPropsMap {
		eventPropMap, isMap := supportedEventVals.(map[string]interface{})

		if !allowEvent || !isMap {
			pkgLogger.Debugf("Previous evaluation of allowAll is false or type assertion failed for an event property(%v), filtering event based on default behaviour", eventProperty)
			allowEvent = evaluatedDefaultBehaviour
			break
		}
		if eventProperty == "messageType" {
			messageTypeEventPropDef := ConvertEventPropMapToStruct[string](eventPropMap)
			// eventPropMap == nil  -- occurs when both allowAll and allowedVals are not defined or when allowAll contains any value other than boolean
			// eventPropMap.AllowAll -- When only allowAll is defined
			//  len(eventPropMap.AllowedVals) == 0 -- when allowedVals is empty array(occurs when it is [] or when data-type of one of the values is not string)
			if eventPropMap == nil || messageTypeEventPropDef.AllowAll || len(messageTypeEventPropDef.AllowedVals) == 0 {
				pkgLogger.Debug("Problem with message type event property map, filtering event based on default behaviour")
				allowEvent = evaluatedDefaultBehaviour
				continue
			}

			pkgLogger.Debugf("SupportedVals: %v -- EventType from event: %v\n", messageTypeEventPropDef.AllowedVals, messageType)
			allowEvent = lo.Contains(messageTypeEventPropDef.AllowedVals, messageType) && evaluatedDefaultBehaviour
		}
	}
	return allowEvent, nil
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
	for _, key := range eventPropertyKeys {
		val, ok := eventPropMap[key]
		if !ok {
			pkgLogger.Debugf(" %q not found in eventPropertiesMap(supportedConnectionModes.sourceType.connectionMode.[eventProperty])", key)
			continue
		}
		switch key {
		case eventProperty_allowAll:
			allowAll, convertable := val.(bool)
			if !convertable {
				return nil
			}
			eventPropertyStruct.AllowAll = allowAll
		case eventProperty_allowedValues:
			allowedVals := ConvertToArrayOfType[T](val)
			eventPropertyStruct.AllowedVals = allowedVals
		}
	}
	return &eventPropertyStruct
}

type supportedEventProperties struct {
	EventPropsMap  map[string]interface{}
	Allow          bool
	FailedResponse *transformer.TransformerResponseT
}

/*
We obtain the supported event properties map data from destination.Definition.Config.supportedConnectionModes
Structure looks like this

	[sourceType]: {
		[connectionMode]: {
			[eventProperty]: [...supportedEventPropertyValues]
		}
	}

@returns

	{
		[eventProperty]: [...supportedEventPropertyValues]
	}
*/
func getSupportedEventPropertiesMap(destinationDefinitionConfig map[string]interface{}, srcType, destConnectionMode string, defaultBehaviour bool) supportedEventProperties {
	supportedConnectionModesI, connModesOk := destinationDefinitionConfig["supportedConnectionModes"]
	if !connModesOk {
		pkgLogger.Debug("supportedConnectionModes not present, filtering event based on default behaviour")
		return supportedEventProperties{Allow: defaultBehaviour}
	}
	supportedConnectionModes := supportedConnectionModesI.(map[string]interface{})

	supportedEventPropsMapI, supportedEventPropsLookupErr := misc.NestedMapLookup(supportedConnectionModes, srcType, destConnectionMode)
	if supportedEventPropsLookupErr != nil {
		if supportedEventPropsLookupErr.Level == 0 {
			// Case 2(refer cases in `FilterUsingSupportedConnectionModes` function)
			pkgLogger.Debugf("Failed with %v for SourceType(%v) while looking up for it in supportedConnectionModes, filtering based on default behaviour", supportedEventPropsLookupErr.Err.Error(), srcType)
			return supportedEventProperties{Allow: defaultBehaviour}
		}
		// Cases 3 & 4(refer cases in `FilterUsingSupportedConnectionModes` function)
		pkgLogger.Debugf("Failed with %v for ConnectionMode(%v) while looking up for it in supportedConnectionModes, filtering out the event", supportedEventPropsLookupErr.Err.Error(), destConnectionMode)
		return supportedEventProperties{Allow: false}
	}

	supportedEventPropsMap, isEventPropsICastableToMap := supportedEventPropsMapI.(map[string]interface{})
	if !isEventPropsICastableToMap || len(supportedEventPropsMap) == 0 {
		// includes Case 5, 7(refer cases in `FilterUsingSupportedConnectionModes` function)
		pkgLogger.Debug("Invalid event properties or event properties are not present, filtering event based on default behaviour")
		return supportedEventProperties{Allow: defaultBehaviour}
	}
	return supportedEventProperties{EventPropsMap: supportedEventPropsMap, Allow: true}
}

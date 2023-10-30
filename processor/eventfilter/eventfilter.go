package eventfilter

import (
	"slices"
	"strings"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

const (
	hybridModeEventsFilterKey = "hybridModeCloudEventsFilter"
	hybridMode                = "hybrid"
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

type AllowTransformerEventParams struct {
	TransformerEvent      *transformer.TransformerEvent
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
func AllowEventToDestTransformation(transformerEvent *transformer.TransformerEvent, supportedMsgTypes []string) (bool, *transformer.TransformerResponse) {
	// MessageType filtering -- STARTS
	messageType := strings.TrimSpace(strings.ToLower(getMessageType(&transformerEvent.Message)))
	if messageType == "" {
		// We will abort the event
		return false, &transformer.TransformerResponse{
			Output: transformerEvent.Message, StatusCode: 400,
			Metadata: transformerEvent.Metadata,
			Error:    "Invalid message type. Type assertion failed",
		}
	}

	isSupportedMsgType := slices.Contains(supportedMsgTypes, messageType)
	if !isSupportedMsgType {
		pkgLogger.Debugw("event filtered out due to unsupported msg types",
			"supportedMsgTypes", supportedMsgTypes, "messageType", messageType,
		)
		// We will not allow the event
		return false, &transformer.TransformerResponse{
			Output: transformerEvent.Message, StatusCode: types.FilterEventCode,
			Metadata: transformerEvent.Metadata,
			Error:    "Message type not supported",
		}
	}
	// MessageType filtering -- ENDS

	// hybridModeCloudEventsFilter.srcType.[eventProperty] filtering -- STARTS
	allow := FilterEventsForHybridMode(ConnectionModeFilterParams{
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
		return allow, &transformer.TransformerResponse{
			Output: transformerEvent.Message, StatusCode: types.FilterEventCode,
			Metadata: transformerEvent.Metadata,
			Error:    "Filtering event based on hybridModeFilter",
		}
	}
	// hybridModeCloudEventsFilter.srcType.[eventProperty] filtering -- ENDS

	return true, nil
}

/*
FilterEventsForHybridMode lets the caller know if the event is allowed to flow through server for a `specific destination`
Introduced to support hybrid-mode event filtering on cloud-side

The template inside `destinationDefinition.Config.hybridModeCloudEventsFilter` would look like this
```

	[sourceType]: {
		[eventProperty]: [...supportedEventPropertyValues]
	}

```

Example:

		{
			...
			"hybridModeCloudEventsFilter": {
	      "web": {
	        "messageType": ["track", "page"]
	      }
	    },
			...
		}
*/
func FilterEventsForHybridMode(connectionModeFilterParams ConnectionModeFilterParams) bool {
	destination := connectionModeFilterParams.Destination
	srcType := strings.TrimSpace(connectionModeFilterParams.SrcType)
	messageType := connectionModeFilterParams.Event.MessageType
	evaluatedDefaultBehaviour := connectionModeFilterParams.DefaultBehaviour

	if srcType == "" {
		pkgLogger.Debug("sourceType is empty string, filtering event based on default behaviour")
		return evaluatedDefaultBehaviour
	}

	destConnModeI := misc.MapLookup(destination.Config, "connectionMode")
	if destConnModeI == nil {
		pkgLogger.Debug("connectionMode not present, filtering event based on default behaviour")
		return evaluatedDefaultBehaviour
	}
	destConnectionMode, isDestConnModeString := destConnModeI.(string)
	if !isDestConnModeString || destConnectionMode != hybridMode {
		pkgLogger.Debugf("Provided connectionMode(%v) is in wrong format or the mode is not %q, filtering event based on default behaviour", destConnModeI, hybridMode)
		return evaluatedDefaultBehaviour
	}

	sourceEventPropertiesI := misc.MapLookup(destination.DestinationDefinition.Config, hybridModeEventsFilterKey, srcType)
	if sourceEventPropertiesI == nil {
		pkgLogger.Debugf("Destination definition config doesn't contain proper values for %[1]v or %[1]v.%[2]v", hybridModeEventsFilterKey, srcType)
		return evaluatedDefaultBehaviour
	}
	eventProperties, isOk := sourceEventPropertiesI.(map[string]interface{})

	if !isOk || len(eventProperties) == 0 {
		pkgLogger.Debugf("'%v.%v' is not correctly defined", hybridModeEventsFilterKey, srcType)
		return evaluatedDefaultBehaviour
	}

	// Flag indicating to let the event pass through
	allowEvent := evaluatedDefaultBehaviour
	for eventProperty, supportedEventVals := range eventProperties {

		if !allowEvent {
			pkgLogger.Debugf("Previous evaluation of allowAll is false or type assertion failed for an event property(%v), filtering event based on default behaviour", eventProperty)
			allowEvent = evaluatedDefaultBehaviour
			break
		}
		if eventProperty == "messageType" {
			messageTypes := ConvertToArrayOfType[string](supportedEventVals)
			if len(messageTypes) == 0 {
				pkgLogger.Debug("Problem with message type event property map, filtering event based on default behaviour")
				allowEvent = evaluatedDefaultBehaviour
				continue
			}

			pkgLogger.Debugf("MessageTypes allowed: %v -- MessageType from event: %v\n", messageTypes, messageType)

			allowEvent = slices.Contains(messageTypes, messageType) && evaluatedDefaultBehaviour
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

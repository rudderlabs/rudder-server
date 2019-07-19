package google

import (
	"encoding/json"
	"log"
	"strings"

	"github.com/rudderlabs/rudder-server/misc"
	uuid "github.com/satori/go.uuid"
)

//HandleT implements the generic wrapper to store any
//local variables etc
type HandleT struct {
	//nothing yet
}

func newGADataEventMap(cat, activity string) *map[string]interface{} {
	id := uuid.NewV4()
	eventMap := &map[string]interface{}{
		"v":   "1",
		"t":   "event",
		"tid": "UA-143161493-1",
		"cid": id.String(),
		"ec":  cat,
		"ea":  activity,
	}
	return eventMap
}

// a factory for creating various GA events map
func newGADataMap(eventType string) *map[string]interface{} {
	switch eventType {
	case "event":
		return newGADataEventMap("", "")

	default:
		return newGADataEventMap("", "")
	}
}

//Returns the mapping for a particular event type for GA, after integration
//module the input data will be from file.
func getGAMapping(eventType string) *map[string]interface{} {
	data := []byte(`{"GA.Track": {"cid": "context.traits.anonymous_id","t": "event","ec": "type","ea": "type"}}`)
	var mapping map[string]interface{}
	err := json.Unmarshal(data, &mapping)
	misc.AssertError(err)
	gaEventMapping := mapping["GA"+"."+eventType].(map[string]interface{})
	return &gaEventMapping
}

func getValueFromNestedMap(key string, nMap *map[string]interface{}) interface{} {

	parts := strings.Split(key, ".")
	len := len(parts)
	tempMap := *nMap
	for _, v := range parts[:len-1] {
		tempMap = tempMap[v].(map[string]interface{})
	}

	return tempMap[parts[len-1]]
}

//Transform is the function every destination wrapper implements
func (ga HandleT) Transform(clientEvent *interface{}) (interface{}, bool) {

	var clientEventMap, ok = misc.GetRudderEventMap(*clientEvent)
	if !ok {
		return nil, false
	}

	gaEvent := *newGADataMap(clientEventMap["event"].(string))
	gaMapping := *getGAMapping(clientEventMap["event"].(string))

	for key := range gaEvent {
		log.Printf("key %s", key)
		_, ok := gaMapping[key]
		if ok {
			if key == "t" {
				gaEvent[key] = "event"
			} else {
				gaEvent[key] = getValueFromNestedMap(gaMapping[key].(string), &clientEventMap)
			}
		}
	}
	return gaEvent, true
}

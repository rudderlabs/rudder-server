package schemas

import (
	"github.com/tidwall/gjson"
)

func GetSheetsHeaderSchema() []interface{} {
	hschema := []interface{}{
		"id",
		"anonymous_id",
		"user_id",
		"traits",
		"context_app_build",
		"context_app_name",
		"context_app_namespace",
		"context_app_version",
		"context_device_id",
		"context_device_manufacturer",
		"context_device_model",
		"context_device_name",
		"context_device_type",
		"context_library_name",
		"context_library_version",
		"context_locale",
		"context_network_carrier",
		"context_network_bluetooth",
		"context_network_cellular",
		"context_network_wifi",
		"context_os_name",
		"context_os_version",
		"context_screen",
		"context_timezone",
		"context_traits",
		"context_userAgent",
		"event_text",
		"event",
		"event_name",
		"sent_at",
		"received_at",
		"timestamp",
		"original_timestamp",
		"properties"}

	return hschema
}

func GetSheetsEvent(parsedJSON gjson.Result) []interface{} {
	message := []interface{}{
		parsedJSON.Get("id").String(),
		parsedJSON.Get("anonymous_id").String(),
		parsedJSON.Get("user_id").String(),
		parsedJSON.Get("traits").String(),
		parsedJSON.Get("context_app_build").String(),
		parsedJSON.Get("context_app_name").String(),
		parsedJSON.Get("context_app_namespace").String(),
		parsedJSON.Get("context_app_version").String(),
		parsedJSON.Get("context_device_id").String(),
		parsedJSON.Get("context_device_manufacturer").String(),
		parsedJSON.Get("context_device_model").String(),
		parsedJSON.Get("context_device_name").String(),
		parsedJSON.Get("context_device_type").String(),
		parsedJSON.Get("context_library_name").String(),
		parsedJSON.Get("context_library_version").String(),
		parsedJSON.Get("context_locale").String(),
		parsedJSON.Get("context_network_carrier").String(),
		parsedJSON.Get("context_network_bluetooth").String(),
		parsedJSON.Get("context_network_cellular").String(),
		parsedJSON.Get("context_network_wifi").String(),
		parsedJSON.Get("context_os_name").String(),
		parsedJSON.Get("context_os_version").String(),
		parsedJSON.Get("context_screen").String(),
		parsedJSON.Get("context_timezone").String(),
		parsedJSON.Get("context_traits").String(),
		parsedJSON.Get("context_userAgent").String(),
		parsedJSON.Get("event_text").String(),
		parsedJSON.Get("event").String(),
		parsedJSON.Get("event_name").String(),
		parsedJSON.Get("sent_at").String(),
		parsedJSON.Get("received_at").String(),
		parsedJSON.Get("timestamp").String(),
		parsedJSON.Get("original_timestamp").String(),
		parsedJSON.Get("properties").String()}

	return message
}

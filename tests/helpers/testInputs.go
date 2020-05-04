package helpers

var BatchPayload = `
{
	"batch": [
		{
		"anonymousId": "49e4bdd1c280bc00",
		"messageId": "msgasdfadsf2er34adfsdf1",
		"channel": "android-sdk",
		"destination_props": {
			"AF": {
			"af_uid": "1566363489499-3377330514807116178"
			}
		},
		"context": {
			"app": {
			"build": "1",
			"name": "RudderAndroidClient",
			"namespace": "com.rudderlabs.android.sdk",
			"version": "1.0"
			},
			"device": {
			"id": "49e4bdd1c280bc00",
			"manufacturer": "Google",
			"model": "Android SDK built for x86",
			"name": "generic_x86"
			},
			"locale": "en-US",
			"network": {
			"carrier": "Android"
			},
			"screen": {
			"density": 420,
			"height": 1794,
			"width": 1080
			},
			"traits": {
			"anonymousId": "49e4bdd1c280bc00"
			},
			"user_agent": "Dalvik/2.1.0 (Linux; U; An  droid 9; Android SDK built for x86 Build/PSR1.180720.075)"
		},
		"event": "Demo Track",
		"integrations": {
			"All": true
		},
		"properties": {
			"label": "Demo Label",
			"category": "Demo Category",
			"value": 5
		},
		"type": "track",
		"originalTimestamp": "2019-08-12T05:08:30.909Z",
		"sentAt": "2019-08-12T05:08:30.909Z"
		}
	]
}
`
var LoadedJson = `
{
  "anonymous_id": "af0d6ca8-ac18-42dc-ae98-cc40ba5b7c4d",
  "category": "Demo Category",
  "context_app_build": "1",
  "context_app_name": "RudderAndroidClient",
  "context_app_namespace": "com.rudderlabs.android.sdk",
  "context_app_version": "1.0",
  "context_device_id": "49e4bdd1c280bc00",
  "context_device_manufacturer": "Google",
  "context_device_model": "Android SDK built for x86",
  "context_device_name": "generic_x86",
  "context_ip": "[::1]:55682",
  "context_locale": "en-US",
  "context_network_carrier": "Android",
  "context_screen_density": 420,
  "context_screen_height": 1794,
  "context_screen_width": 1080,
  "context_traits_anonymousId": "49e4bdd1c280bc00",
  "context_user_agent": "Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)",
  "event": "ginkgo",
  "event_text": "ginkgo",
  "id": "74b582ef-7663-4cb3-8666-2273fe5a8ccb",
  "label": "Demo Label",
  "original_timestamp": "2019-08-12T05:08:30.909Z",
  "received_at": "2020-03-25T15:51:24.971Z",
  "sent_at": "2019-08-12T05:08:30.909Z",
  "timestamp": "2020-03-25T15:51:24.971Z",
  "uuid_ts": "2020-03-25 21:32:29 Z",
  "value": 5
}
`

var IdentifyPayload = `
{
	"anonymousId": "49e4bdd1c280bc00",
	"messageId": "msgasdfadsf2er34adfsdf2",
	"type": "identify",
	"userId": "98234023840234",
	"traits": {
	  "name": "Chandra",
	  "email": "chandra@rudderlabs.com",
	  "org": "rudder"
	}
}
`

var AliasPayload = `
{
	"anonymousId": "49e4bdd1c280bc00",
	"messageId": "msgasdfadsf2er34adfsdf3",
	"type": "alias",
	"previousId": "chandra@rudderlabs.com",
	"userId": "98234023840234"
}
`

var TrackPayload = `
{
	"anonymousId": "49e4bdd1c280bc00",
	"messageId": "msgasdfadsf2er34adfsdf4",
	"type": "track",
	"event": "test event",
	"properties": {
		"name": "Chandra"
	}
}
`

var GroupPayload = `
{
	"anonymousId": "49e4bdd1c280bc00",
	"messageId": "msgasdfadsf2er34adfsdf5",
	"type": "group",
	"groupId": "98234023840234adf2e232",
	"traits": {
		"name": "Chandra",
		"email": "chandra@rudderlabs.com",
		"org": "rudder"
	}
}
`

var PagePayload = `
{
	"anonymousId": "49e4bdd1c280bc00",
	"messageId": "msgasdfadsf2er34adfsdf6",
	"type": "page",
	"name": "Hello",
	"properties": {
	  "title": "Welcome to rudder",
	  "url": "http://www.rudderstack.com"
	}
  }
`

var ScreenPayload = `
{
	"anonymousId": "49e4bdd1c280bc00",
	"messageId": "msgasdfadsf2er34adfsdf7",
	"type": "screen",
	"name": "Hello",
	"properties": {
		"title": "Welcome to rudder"
	}
}
`

var WarehouseBatchPayload = `
{
	"batch": [
		{
		"anonymousId": "49e4bdd1c280bc00",
		"messageId": "msgasdfadsf2er34adfsdf1",
		"channel": "android-sdk",
		"destination_props": {
			"AF": {
			"af_uid": "1566363489499-3377330514807116178"
			}
		},
		"context": {
			"app": {
			"build": "1",
			"name": "RudderAndroidClient",
			"namespace": "com.rudderlabs.android.sdk",
			"version": "1.0"
			},
			"device": {
			"id": "49e4bdd1c280bc00",
			"manufacturer": "Google",
			"model": "Android SDK built for x86",
			"name": "generic_x86"
			},
			"locale": "en-US",
			"network": {
			"carrier": "Android"
			},
			"screen": {
			"density": 420,
			"height": 1794,
			"width": 1080
			},
			"traits": {
			"anonymousId": "49e4bdd1c280bc00"
			},
			"user_agent": "Dalvik/2.1.0 (Linux; U; An  droid 9; Android SDK built for x86 Build/PSR1.180720.075)"
		},
		"event": "Demo Track",
		"integrations": {
			"All": true
		},
		"properties": {
			"label": "Demo Label",
			"category": "Demo Category",
			"value": 5,
			"property1":"test",
			"property2":"test",
			"property3":"test",
			"property4":"test",
			"property5":"test"
		},
		"type": "track",
		"originalTimestamp": "2019-08-12T05:08:30.909Z",
		"sentAt": "2019-08-12T05:08:30.909Z"
		}
	]
}
`


var RedshiftSchema= map[string]map[string]string {
  "ginkgo": {
    "id": "string",
    "event": "string",
    "label": "string",
    "value": "int",
    "channel": "string",
    "sent_at": "datetime",
    "uuid_ts": "datetime",
    "category": "string",
    "property1": "string",
    "property2": "string",
    "property3": "string",
    "property4": "string",
    "property5": "string",
    "_timestamp": "datetime",
    "context_ip": "string",
    "event_text": "string",
    "received_at": "datetime",
    "anonymous_id": "string",
    "context_locale": "string",
    "context_app_name": "string",
    "context_app_build": "string",
    "context_device_id": "string",
    "context_user_agent": "string",
    "original_timestamp": "datetime",
    "context_app_version": "string",
    "context_device_name": "string",
    "context_device_model": "string",
    "context_screen_width": "int",
    "context_app_namespace": "string",
    "context_screen_height": "int",
    "context_screen_density": "int",
    "context_network_carrier": "string",
    "context_traits_anonymousId": "string",
    "context_device_manufacturer": "string",
  },
  "tracks": {
    "id": "string",
    "event": "string",
    "channel": "string",
    "sent_at": "datetime",
    "uuid_ts": "datetime",
    "_timestamp": "datetime",
    "context_ip": "string",
    "event_text": "string",
    "received_at": "datetime",
    "anonymous_id": "string",
    "context_locale": "string",
    "context_app_name": "string",
    "context_app_build": "string",
    "context_device_id": "string",
    "context_user_agent": "string",
    "original_timestamp": "datetime",
    "context_app_version": "string",
    "context_device_name": "string",
    "context_device_model": "string",
    "context_screen_width": "int",
    "context_app_namespace": "string",
    "context_screen_height": "int",
    "context_screen_density": "int",
    "context_network_carrier": "string",
    "context_traits_anonymousId": "string",
    "context_device_manufacturer": "string",
  },
  "rudder_discards": {
    "row_id": "string",
    "uuid_ts": "datetime",
    "table_name": "string",
    "column_name": "string",
    "received_at": "datetime",
    "column_value": "string",
  },
}
var ReservedKeywordsRedshiftSchema= map[string]map[string]string {
	"ginkgo": {
		"id": "string",
		"_from": "string",
		"_join": "string",
		"event": "string",
		"label": "string",
		"value": "int",
		"_order": "string",
		"_where": "string",
		"_select": "string",
		"channel": "string",
		"sent_at": "datetime",
		"uuid_ts": "datetime",
		"category": "string",
		"property1": "string",
		"property2": "string",
		"property3": "string",
		"property4": "string",
		"property5": "string",
		"_timestamp": "datetime",
		"context_ip": "string",
		"event_text": "string",
		"received_at": "datetime",
		"anonymous_id": "string",
		"context_locale": "string",
		"context_app_name": "string",
		"context_app_build": "string",
		"context_device_id": "string",
		"context_user_agent": "string",
		"original_timestamp": "datetime",
		"context_app_version": "string",
		"context_device_name": "string",
		"context_device_model": "string",
		"context_screen_width": "int",
		"context_app_namespace": "string",
		"context_screen_height": "int",
		"context_screen_density": "int",
		"context_network_carrier": "string",
		"context_traits_anonymousId": "string",
		"context_device_manufacturer": "string",
	},
	"tracks": {
		"id": "string",
		"event": "string",
		"channel": "string",
		"sent_at": "datetime",
		"uuid_ts": "datetime",
		"_timestamp": "datetime",
		"context_ip": "string",
		"event_text": "string",
		"received_at": "datetime",
		"anonymous_id": "string",
		"context_locale": "string",
		"context_app_name": "string",
		"context_app_build": "string",
		"context_device_id": "string",
		"context_user_agent": "string",
		"original_timestamp": "datetime",
		"context_app_version": "string",
		"context_device_name": "string",
		"context_device_model": "string",
		"context_screen_width": "int",
		"context_app_namespace": "string",
		"context_screen_height": "int",
		"context_screen_density": "int",
		"context_network_carrier": "string",
		"context_traits_anonymousId": "string",
		"context_device_manufacturer": "string",
	},
	"rudder_discards": {
		"row_id": "string",
		"uuid_ts": "datetime",
		"table_name": "string",
		"column_name": "string",
		"received_at": "datetime",
		"column_value": "string",
	},
}

var BigQuerySchema = map[string]map[string]string {
  "ginkgo": {
    "id": "string",
    "event": "string",
    "label": "string",
    "value": "int",
    "channel": "string",
    "sent_at": "datetime",
    "uuid_ts": "datetime",
    "category": "string",
    "property1": "string",
    "property2": "string",
    "property3": "string",
    "property4": "string",
    "property5": "string",
    "timestamp": "datetime",
    "context_ip": "string",
    "event_text": "string",
    "received_at": "datetime",
    "anonymous_id": "string",
    "context_locale": "string",
    "context_app_name": "string",
    "context_app_build": "string",
    "context_device_id": "string",
    "context_user_agent": "string",
    "original_timestamp": "datetime",
    "context_app_version": "string",
    "context_device_name": "string",
    "context_device_model": "string",
    "context_screen_width": "int",
    "context_app_namespace": "string",
    "context_screen_height": "int",
    "context_screen_density": "int",
    "context_network_carrier": "string",
    "context_traits_anonymousId": "string",
    "context_device_manufacturer": "string",
  },
  "tracks": {
    "id": "string",
    "event": "string",
    "channel": "string",
    "sent_at": "datetime",
    "uuid_ts": "datetime",
    "timestamp": "datetime",
    "context_ip": "string",
    "event_text": "string",
    "received_at": "datetime",
    "anonymous_id": "string",
    "context_locale": "string",
    "context_app_name": "string",
    "context_app_build": "string",
    "context_device_id": "string",
    "context_user_agent": "string",
    "original_timestamp": "datetime",
    "context_app_version": "string",
    "context_device_name": "string",
    "context_device_model": "string",
    "context_screen_width": "int",
    "context_app_namespace": "string",
    "context_screen_height": "int",
    "context_screen_density": "int",
    "context_network_carrier": "string",
    "context_traits_anonymousId": "string",
    "context_device_manufacturer": "string",
  },
  "rudder_discards": {
    "row_id": "string",
    "uuid_ts": "datetime",
    "table_name": "string",
    "column_name": "string",
    "received_at": "datetime",
    "column_value": "string",
  },
}
var ReserverKeyWordsBigQuerySchema = map[string]map[string]string {
	"ginkgo": {
		"id": "string",
		"_from": "string",
		"_join": "string",
		"event": "string",
		"label": "string",
		"value": "int",
		"_order": "string",
		"_where": "string",
		"_select": "string",
		"channel": "string",
		"sent_at": "datetime",
		"uuid_ts": "datetime",
		"category": "string",
		"property1": "string",
		"property2": "string",
		"property3": "string",
		"property4": "string",
		"property5": "string",
		"timestamp": "datetime",
		"context_ip": "string",
		"event_text": "string",
		"received_at": "datetime",
		"anonymous_id": "string",
		"context_locale": "string",
		"context_app_name": "string",
		"context_app_build": "string",
		"context_device_id": "string",
		"context_user_agent": "string",
		"original_timestamp": "datetime",
		"context_app_version": "string",
		"context_device_name": "string",
		"context_device_model": "string",
		"context_screen_width": "int",
		"context_app_namespace": "string",
		"context_screen_height": "int",
		"context_screen_density": "int",
		"context_network_carrier": "string",
		"context_traits_anonymousId": "string",
		"context_device_manufacturer": "string",
	},
	"tracks": {
		"id": "string",
		"event": "string",
		"channel": "string",
		"sent_at": "datetime",
		"uuid_ts": "datetime",
		"timestamp": "datetime",
		"context_ip": "string",
		"event_text": "string",
		"received_at": "datetime",
		"anonymous_id": "string",
		"context_locale": "string",
		"context_app_name": "string",
		"context_app_build": "string",
		"context_device_id": "string",
		"context_user_agent": "string",
		"original_timestamp": "datetime",
		"context_app_version": "string",
		"context_device_name": "string",
		"context_device_model": "string",
		"context_screen_width": "int",
		"context_app_namespace": "string",
		"context_screen_height": "int",
		"context_screen_density": "int",
		"context_network_carrier": "string",
		"context_traits_anonymousId": "string",
		"context_device_manufacturer": "string",
	},
	"rudder_discards": {
		"row_id": "string",
		"uuid_ts": "datetime",
		"table_name": "string",
		"column_name": "string",
		"received_at": "datetime",
		"column_value": "string",
	},
}

var SnowflakeSchema = map[string]map[string]string {
  "GINKGO": {
    "ID": "string",
    "EVENT": "string",
    "LABEL": "string",
    "VALUE": "int",
    "CHANNEL": "string",
    "SENT_AT": "datetime",
    "UUID_TS": "datetime",
    "CATEGORY": "string",
    "PROPERTY1": "string",
    "PROPERTY2": "string",
    "PROPERTY3": "string",
    "PROPERTY4": "string",
    "PROPERTY5": "string",
    "TIMESTAMP": "datetime",
    "CONTEXT_IP": "string",
    "EVENT_TEXT": "string",
    "RECEIVED_AT": "datetime",
    "ANONYMOUS_ID": "string",
    "CONTEXT_LOCALE": "string",
    "CONTEXT_APP_NAME": "string",
    "CONTEXT_APP_BUILD": "string",
    "CONTEXT_DEVICE_ID": "string",
    "CONTEXT_USER_AGENT": "string",
    "ORIGINAL_TIMESTAMP": "datetime",
    "CONTEXT_APP_VERSION": "string",
    "CONTEXT_DEVICE_NAME": "string",
    "CONTEXT_DEVICE_MODEL": "string",
    "CONTEXT_SCREEN_WIDTH": "int",
    "CONTEXT_APP_NAMESPACE": "string",
    "CONTEXT_SCREEN_HEIGHT": "int",
    "CONTEXT_SCREEN_DENSITY": "int",
    "CONTEXT_NETWORK_CARRIER": "string",
    "CONTEXT_TRAITS_ANONYMOUSID": "string",
    "CONTEXT_DEVICE_MANUFACTURER": "string",
  },
  "TRACKS": {
    "ID": "string",
    "EVENT": "string",
    "CHANNEL": "string",
    "SENT_AT": "datetime",
    "UUID_TS": "datetime",
    "TIMESTAMP": "datetime",
    "CONTEXT_IP": "string",
    "EVENT_TEXT": "string",
    "RECEIVED_AT": "datetime",
    "ANONYMOUS_ID": "string",
    "CONTEXT_LOCALE": "string",
    "CONTEXT_APP_NAME": "string",
    "CONTEXT_APP_BUILD": "string",
    "CONTEXT_DEVICE_ID": "string",
    "CONTEXT_USER_AGENT": "string",
    "ORIGINAL_TIMESTAMP": "datetime",
    "CONTEXT_APP_VERSION": "string",
    "CONTEXT_DEVICE_NAME": "string",
    "CONTEXT_DEVICE_MODEL": "string",
    "CONTEXT_SCREEN_WIDTH": "int",
    "CONTEXT_APP_NAMESPACE": "string",
    "CONTEXT_SCREEN_HEIGHT": "int",
    "CONTEXT_SCREEN_DENSITY": "int",
    "CONTEXT_NETWORK_CARRIER": "string",
    "CONTEXT_TRAITS_ANONYMOUSID": "string",
    "CONTEXT_DEVICE_MANUFACTURER": "string",
  },
  "RUDDER_DISCARDS": {
    "ROW_ID": "string",
    "UUID_TS": "datetime",
    "TABLE_NAME": "string",
    "COLUMN_NAME": "string",
    "RECEIVED_AT": "datetime",
    "COLUMN_VALUE": "string",
  },
}
var ReservedKeywordsSnowflakeSchema = map[string]map[string]string {
	"GINKGO": {
		"ID": "string",
		"EVENT": "string",
		"LABEL": "string",
		"VALUE": "int",
		"_FROM": "string",
		"_JOIN": "string",
		"_ORDER": "string",
		"_WHERE": "string",
		"CHANNEL": "string",
		"SENT_AT": "datetime",
		"UUID_TS": "datetime",
		"_SELECT": "string",
		"CATEGORY": "string",
		"PROPERTY1": "string",
		"PROPERTY2": "string",
		"PROPERTY3": "string",
		"PROPERTY4": "string",
		"PROPERTY5": "string",
		"TIMESTAMP": "datetime",
		"CONTEXT_IP": "string",
		"EVENT_TEXT": "string",
		"RECEIVED_AT": "datetime",
		"ANONYMOUS_ID": "string",
		"CONTEXT_LOCALE": "string",
		"CONTEXT_APP_NAME": "string",
		"CONTEXT_APP_BUILD": "string",
		"CONTEXT_DEVICE_ID": "string",
		"CONTEXT_USER_AGENT": "string",
		"ORIGINAL_TIMESTAMP": "datetime",
		"CONTEXT_APP_VERSION": "string",
		"CONTEXT_DEVICE_NAME": "string",
		"CONTEXT_DEVICE_MODEL": "string",
		"CONTEXT_SCREEN_WIDTH": "int",
		"CONTEXT_APP_NAMESPACE": "string",
		"CONTEXT_SCREEN_HEIGHT": "int",
		"CONTEXT_SCREEN_DENSITY": "int",
		"CONTEXT_NETWORK_CARRIER": "string",
		"CONTEXT_TRAITS_ANONYMOUSID": "string",
		"CONTEXT_DEVICE_MANUFACTURER": "string",
	},
	"TRACKS": {
		"ID": "string",
		"EVENT": "string",
		"CHANNEL": "string",
		"SENT_AT": "datetime",
		"UUID_TS": "datetime",
		"TIMESTAMP": "datetime",
		"CONTEXT_IP": "string",
		"EVENT_TEXT": "string",
		"RECEIVED_AT": "datetime",
		"ANONYMOUS_ID": "string",
		"CONTEXT_LOCALE": "string",
		"CONTEXT_APP_NAME": "string",
		"CONTEXT_APP_BUILD": "string",
		"CONTEXT_DEVICE_ID": "string",
		"CONTEXT_USER_AGENT": "string",
		"ORIGINAL_TIMESTAMP": "datetime",
		"CONTEXT_APP_VERSION": "string",
		"CONTEXT_DEVICE_NAME": "string",
		"CONTEXT_DEVICE_MODEL": "string",
		"CONTEXT_SCREEN_WIDTH": "int",
		"CONTEXT_APP_NAMESPACE": "string",
		"CONTEXT_SCREEN_HEIGHT": "int",
		"CONTEXT_SCREEN_DENSITY": "int",
		"CONTEXT_NETWORK_CARRIER": "string",
		"CONTEXT_TRAITS_ANONYMOUSID": "string",
		"CONTEXT_DEVICE_MANUFACTURER": "string",
	},
	"RUDDER_DISCARDS": {
		"ROW_ID": "string",
		"UUID_TS": "datetime",
		"TABLE_NAME": "string",
		"COLUMN_NAME": "string",
		"RECEIVED_AT": "datetime",
		"COLUMN_VALUE": "string",
	},
}


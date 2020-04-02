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

var BQBatchPayload = `
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
			"name": "SELECT",
			"namespace": "com.rudderlabs.android.sdk",
			"version": "1.0"
			},
			"device": {
			"id": "49e4bdd1c280bc00",
			"manufacturer": "Google",
			"model": "Omega",
			"name": "generic_x86"
			},
			"locale": "en-US",
			"network": {
			"carrier": "Nokia"
			},
			"screen": {
			"density": 420,
			"height": 1794,
			"width": 1080
			},
			"traits": {
			"anonymousId": "49e4bdd1c280bc00"
			},
			"user_agent": "Black Mamba"
		},
		"event": "JOIN",
		"integrations": {
			"All": true
		},
		"properties": {
			"MERGE": "LIMIT",
			"NATURAL": "TRUE",
			"HAVING": "BETWEEN",
			"WHERE":"HERE"
		},
		"type": "track",
		"originalTimestamp": "2019-08-12T05:08:30.909Z",
		"sentAt": "2019-08-12T05:08:30.909Z"
		}
	]
}
`

var DiffStringFormatBatchPayload = `
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
			"text": "Ken\"ny\"s iPh'o\"ne5\",6"
		},
		"type": "track",
		"originalTimestamp": "2019-08-12T05:08:30.909Z",
		"sentAt": "2019-08-12T05:08:30.909Z"
		}
	]
}
`
var DTBatchPayload = `
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
		},
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
			"build": 0.9.8,
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
			"user_agent": 1
		},
		"event": "Demo Track",
		"integrations": {
			"All": true
		},
		"properties": {
			"label": "Demo Label",
			"category": "Demo Category",
			"value": 5.09
		},
		"type": "track",
		"originalTimestamp": "2019-08-12T05:08:30.909Z",
		"sentAt": "2019-08-12T05:08:30.909Z"
		},{
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
			"version": 1.0
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
			"height": "1794",
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
			"value": 5.0
		},
		"type": "track",
		"originalTimestamp": "2019-08-12T05:08:30.909Z",
		"sentAt": "2019-08-12T05:08:30.909Z"
		}

	]
}
`
var DTSchema = map[string]map[string]string{
	"ginkgo": {
		"id":                          "string",
		"event":                       "string",
		"label":                       "string",
		"value":                       "int",
		"sent_at":                     "datetime",
		"uuid_ts":                     "datetime",
		"category":                    "string",
		"timestamp":                   "datetime",
		"context_ip":                  "string",
		"event_text":                  "string",
		"received_at":                 "datetime",
		"anonymous_id":                "string",
		"context_locale":              "string",
		"context_app_name":            "string",
		"context_app_build":           "string",
		"context_device_id":           "string",
		"context_user_agent":          "string",
		"original_timestamp":          "datetime",
		"context_app_version":         "string",
		"context_device_name":         "string",
		"context_device_model":        "string",
		"context_screen_width":        "int",
		"context_app_namespace":       "string",
		"context_screen_height":       "int",
		"context_screen_density":      "int",
		"context_network_carrier":     "string",
		"context_traits_anonymousId":  "string",
		"context_device_manufacturer": "string",
	},
	"tracks": {
		"id":                          "string",
		"event":                       "string",
		"sent_at":                     "datetime",
		"uuid_ts":                     "datetime",
		"timestamp":                   "datetime",
		"context_ip":                  "string",
		"event_text":                  "string",
		"received_at":                 "datetime",
		"anonymous_id":                "string",
		"context_locale":              "string",
		"context_app_name":            "string",
		"context_app_build":           "string",
		"context_device_id":           "string",
		"context_user_agent":          "string",
		"original_timestamp":          "datetime",
		"context_app_version":         "string",
		"context_device_name":         "string",
		"context_device_model":        "string",
		"context_screen_width":        "int",
		"context_app_namespace":       "string",
		"context_screen_height":       "int",
		"context_screen_density":      "int",
		"context_network_carrier":     "string",
		"context_traits_anonymousId":  "string",
		"context_device_manufacturer": "string",
	},
}

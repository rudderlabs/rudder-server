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
			"user_agent": "Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"
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

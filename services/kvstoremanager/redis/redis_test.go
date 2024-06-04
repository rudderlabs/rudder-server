package redis

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"
)

type nonRootInsertExpectation struct {
	setArgsPath string
	updatedVal  string
	jsonMap     string
}
type nonRootInsertTestCase struct {
	description string
	jsonData    string // transformed value
	userVal     string // value we are getting from redis for a key
	expected    nonRootInsertExpectation
}

func TestNonRootInsert(t *testing.T) {
	nonRootInsertTcs := []nonRootInsertTestCase{
		{
			description: "userVal not empty, one of parent keys in path empty",
			jsonData:    `{"message": {"key": "user:1", "path": "profile.user.details.name", "value": {"first": "John", "last": "Doe"}}}`,
			userVal:     `{"profile":{"id": "uiuide1134"}}`,
			expected: nonRootInsertExpectation{
				setArgsPath: "$.profile",
				updatedVal:  `{"id":"uiuide1134"}`,
				jsonMap:     `{"user":{"details":{"name":{"first": "John", "last": "Doe"}}}}`,
			},
		},
		{
			description: "userVal empty, path is non-empty",
			jsonData:    `{"message": {"key": "user:2", "path": "profile.user.details.name", "value": {"first": "John", "last": "Doe"}}}`,
			expected: nonRootInsertExpectation{
				setArgsPath: "$",
				updatedVal:  "{}",
				jsonMap:     `{"profile":{"user":{"details":{"name":{"first": "John", "last": "Doe"}}}}}`,
			},
		},
		{
			description: "userVal is empty, path empty(not sent)",
			jsonData:    `{"message": {"key": "user:1", "value": {"first": "John", "last": "Doe"}}}`,
			expected: nonRootInsertExpectation{
				setArgsPath: "$",
				updatedVal:  "{}",
				jsonMap:     `{"first": "John", "last": "Doe"}`,
			},
		},
		{
			description: "userVal not empty, only of child key in path empty",
			jsonData:    `{"message": {"key": "user:1", "path": "profile.details.name", "value": {"first": "John", "last": "Doe"}}}`,
			userVal:     `{"profile":{"id": "uiuide1134","details":{"formNo":123545}}}`,
			expected: nonRootInsertExpectation{
				setArgsPath: "$.profile.details",
				updatedVal:  `{"formNo":123545}`,
				jsonMap:     `{"name":{"first": "John", "last": "Doe"}}`,
			},
		},
		{
			description: "userVal not empty, only of child key in path not empty",
			jsonData:    `{"message":{"key":"user:1", "path":"profile.details.name","value":{"first":"John","last":"Doe"}}}`,
			userVal:     `{"profile":{"id": "uiuide1134","details":{"formNo":123545,"name":{"first":"Jane"}}}}`,
			expected: nonRootInsertExpectation{
				setArgsPath: "$.profile.details.name",
				updatedVal:  `{"first":"Jane"}`,
				jsonMap:     `{"first": "John", "last": "Doe"}`,
			},
		},
		{
			description: "userVal not empty, first parent key itself in path is empty",
			jsonData:    `{"message":{"key":"user:1", "path":"profile.details.name","value":{"first":"John","last":"Doe"}}}`,
			userVal:     `{"id": "uiuide1134"}`,
			expected: nonRootInsertExpectation{
				setArgsPath: "$",
				updatedVal:  `{"id": "uiuide1134"}`,
				jsonMap:     `{"profile":{"details":{"name":{"first": "John", "last": "Doe"}}}}`,
			},
		},
		{
			description: "userVal empty, first parent key itself in path is empty",
			jsonData:    `{"message":{"key":"user:1", "path":"profile.details.name","value":{"first":"John","last":"Doe"}}}`,
			expected: nonRootInsertExpectation{
				setArgsPath: "$",
				updatedVal:  `{}`,
				jsonMap:     `{"profile":{"details":{"name":{"first": "John", "last": "Doe"}}}}`,
			},
		},
		{
			description: "userVal not empty, parent key `profile` is not a json",
			jsonData:    `{"message": {"key": "user:1", "path": "profile.user.details.name", "value": {"first": "John", "last": "Doe"}}}`,
			userVal:     `{"profile":"something"}`,
			expected: nonRootInsertExpectation{
				setArgsPath: "$.profile",
				updatedVal:  `"something"`,
				jsonMap:     `{"user":{"details":{"name":{"first": "John", "last": "Doe"}}}}`,
			},
		},
	}
	for _, tc := range nonRootInsertTcs {
		t.Run(tc.description, func(t *testing.T) {
			jsonData := json.RawMessage(tc.jsonData)
			path := gjson.GetBytes(jsonData, "message.path").String()
			jsonVal := gjson.GetBytes(jsonData, "message.value")

			redisMgr := &RedisManager{}
			insertRet, err := redisMgr.HandleNonRootInsert(NonRootInsertParams{
				valueInRedis: tc.userVal,
				Path: path,
				JsonVal: jsonVal,
			})
			assert.NoError(t, err)
			assert.Equal(t, tc.expected.setArgsPath, insertRet.SetArgsPath)

			var expectedUpdateVal map[string]interface{}
			updUnmarshalErr := json.Unmarshal([]byte(tc.expected.updatedVal), &expectedUpdateVal)
			if updUnmarshalErr != nil {
				assert.Equal(t, tc.expected.updatedVal, insertRet.MergeTo)
				goto checkforJsonMap
			}
			assert.JSONEq(t, tc.expected.updatedVal, insertRet.MergeTo)

		checkforJsonMap:
			actualBytes, marshalErr := json.Marshal(insertRet.MergeFrom)
			assert.NoError(t, marshalErr)
			assert.JSONEq(t, tc.expected.jsonMap, string(actualBytes))
		})
	}
}

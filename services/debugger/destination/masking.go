package destinationdebugger

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
)

const maskedValue = "******"

// maskListedPaths replaces existing values at paths. It falls back to mask-all on malformed
// payloads or any path application error, so a masking failure never forwards the original value.
func maskListedPaths(payload json.RawMessage, paths []string) (json.RawMessage, bool) {
	if len(paths) == 0 {
		return payload, false
	}
	if !isJSONObject(payload) {
		return maskAll(payload)
	}

	masked := bytes.Clone(payload)
	for _, path := range paths {
		if targetsEndpoint(path) {
			continue
		}
		if path == "" {
			fallback, _ := maskAll(masked)
			return fallback, true
		}
		if !gjson.GetBytes(masked, path).Exists() {
			continue
		}

		if parentPath, ok := terminalArrayWildcardParent(path); ok {
			var maskErr bool
			masked, maskErr = maskTerminalArrayWildcard(masked, parentPath)
			if maskErr {
				fallback, _ := maskAll(masked)
				return fallback, true
			}
			continue
		}

		var err error
		masked, err = sjson.SetBytes(masked, path, maskedValue)
		if err != nil || !pathMasked(masked, path) {
			fallback, _ := maskAll(masked)
			return fallback, true
		}
	}
	return masked, false
}

func maskTerminalArrayWildcard(payload json.RawMessage, parentPath string) (json.RawMessage, bool) {
	array := gjson.GetBytes(payload, parentPath)
	if !array.IsArray() {
		return payload, true
	}

	masked := payload
	for idx := range array.Array() {
		concretePath := appendPathSegment(parentPath, strconv.Itoa(idx))
		var err error
		masked, err = sjson.SetBytes(masked, concretePath, maskedValue)
		if err != nil || !pathMasked(masked, concretePath) {
			return masked, true
		}
	}
	return masked, false
}

func pathMasked(payload json.RawMessage, path string) bool {
	result := gjson.GetBytes(payload, path)
	return result.Exists() && resultMasked(result)
}

func resultMasked(result gjson.Result) bool {
	if result.IsArray() {
		for _, item := range result.Array() {
			if !resultMasked(item) {
				return false
			}
		}
		return true
	}
	return result.Type == gjson.String && result.String() == maskedValue
}

// maskAll preserves endpoint and replaces every other existing top-level value. Invalid JSON and
// non-object JSON are replaced wholesale with a valid masked JSON string.
func maskAll(payload json.RawMessage) (json.RawMessage, bool) {
	if !isJSONObject(payload) {
		return json.RawMessage(`"******"`), true
	}

	var rebuilt bytes.Buffer
	rebuilt.WriteByte('{')
	var maskingErr bool
	first := true
	gjson.ParseBytes(payload).ForEach(func(key, value gjson.Result) bool {
		if !first {
			rebuilt.WriteByte(',')
		}
		first = false

		keyBytes, err := jsonrs.Marshal(key.String())
		if err != nil {
			maskingErr = true
			return false
		}
		rebuilt.Write(keyBytes)
		rebuilt.WriteByte(':')
		if key.String() == "endpoint" {
			rebuilt.WriteString(value.Raw)
		} else {
			rebuilt.WriteString(`"******"`)
		}
		return true
	})
	if maskingErr {
		return json.RawMessage(`"******"`), true
	}
	rebuilt.WriteByte('}')
	return rebuilt.Bytes(), false
}

func isJSONObject(payload json.RawMessage) bool {
	if !gjson.ValidBytes(payload) {
		return false
	}
	trimmed := bytes.TrimSpace(payload)
	return len(trimmed) > 0 && trimmed[0] == '{'
}

func targetsEndpoint(path string) bool {
	return path == "endpoint" || len(path) > len("endpoint") && path[:len("endpoint")+1] == "endpoint."
}

func terminalArrayWildcardParent(path string) (string, bool) {
	segments := splitPath(path)
	if len(segments) == 0 || segments[len(segments)-1] != "#" {
		return "", false
	}
	return strings.Join(segments[:len(segments)-1], "."), true
}

func splitPath(path string) []string {
	segments := make([]string, 0, strings.Count(path, ".")+1)
	var segment strings.Builder
	escaped := false
	for _, r := range path {
		if escaped {
			segment.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' {
			segment.WriteRune(r)
			escaped = true
			continue
		}
		if r == '.' {
			segments = append(segments, segment.String())
			segment.Reset()
			continue
		}
		segment.WriteRune(r)
	}
	segments = append(segments, segment.String())
	return segments
}

func appendPathSegment(path, segment string) string {
	if path == "" {
		return segment
	}
	return path + "." + segment
}

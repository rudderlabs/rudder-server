package router

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const maskedValue = "******"

var errPathNotMasked = errors.New("path not masked")

func (w *worker) maskDeliveryPayload(payload json.RawMessage) json.RawMessage {
	if w.rt.reloadableConfig.disableEventDeliveryUploadMasking.Load() {
		return payload
	}

	maskingCounter := w.rt.deliveryPayloadMaskingAllCounter
	var maskErr bool
	if paths, ok := w.rt.transformerFeaturesService.SecretPaths(w.rt.destType); ok {
		maskingCounter = w.rt.deliveryPayloadMaskingListedCounter
		payload, maskErr = maskListedPaths(payload, paths)
	} else {
		payload, maskErr = maskAll(payload)
	}
	maskingCounter.Increment()
	if maskErr {
		w.rt.deliveryPayloadMaskingErrorCounter.Increment()
	}
	return payload
}

// maskListedPaths replaces existing values at paths. It falls back to mask-all on malformed
// payloads or any path application error, so a masking failure never forwards the original value.
func maskListedPaths(payload json.RawMessage, paths []string) (json.RawMessage, bool) {
	if len(paths) == 0 {
		return payload, false
	}
	if !isJSONObject(payload) {
		return maskAll(payload)
	}
	masked, err := applyListedPaths(payload, paths)
	if err != nil {
		fallback, _ := maskAll(masked)
		return fallback, true
	}
	return masked, false
}

// applyListedPaths never mutates payload: sjson allocates a new slice unless ReplaceInPlace is set.
func applyListedPaths(masked json.RawMessage, paths []string) (json.RawMessage, error) {
	for _, path := range paths {
		if targetsEndpoint(path) {
			continue
		}
		if path == "" {
			return masked, errPathNotMasked
		}
		if !gjson.GetBytes(masked, path).Exists() {
			continue
		}

		if parentPath, ok := terminalArrayWildcardParent(path); ok {
			array := gjson.GetBytes(masked, parentPath)
			if !array.IsArray() {
				return masked, errPathNotMasked
			}
			for idx := range array.Array() {
				var err error
				if masked, err = setMasked(masked, appendPathSegment(parentPath, strconv.Itoa(idx))); err != nil {
					return masked, err
				}
			}
			continue
		}

		var err error
		if masked, err = setMasked(masked, path); err != nil {
			return masked, err
		}
	}
	return masked, nil
}

func setMasked(payload json.RawMessage, path string) (json.RawMessage, error) {
	masked, err := sjson.SetBytes(payload, path, maskedValue)
	if err != nil {
		return masked, err
	}
	if !resultMasked(gjson.GetBytes(masked, path)) {
		return masked, errPathNotMasked
	}
	return masked, nil
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
	first := true
	gjson.ParseBytes(payload).ForEach(func(key, value gjson.Result) bool {
		if !first {
			rebuilt.WriteByte(',')
		}
		first = false

		rebuilt.WriteString(key.Raw)
		rebuilt.WriteByte(':')
		if key.String() == "endpoint" {
			rebuilt.WriteString(value.Raw)
		} else {
			rebuilt.WriteString(`"******"`)
		}
		return true
	})
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
	return path == "endpoint"
}

// terminalArrayWildcardParent returns the parent of a path whose last segment is an unescaped "#".
func terminalArrayWildcardParent(path string) (string, bool) {
	if path == "#" {
		return "", true
	}
	parent, ok := strings.CutSuffix(path, ".#")
	if !ok {
		return "", false
	}
	// an odd run of trailing backslashes escapes the dot, making "#" part of the previous key
	if trailingBackslashes := len(parent) - len(strings.TrimRight(parent, `\`)); trailingBackslashes%2 == 1 {
		return "", false
	}
	return parent, true
}

func appendPathSegment(path, segment string) string {
	if path == "" {
		return segment
	}
	return path + "." + segment
}

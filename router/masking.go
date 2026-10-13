package router

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	maskedValue = "******"
	maskedJSON  = `"` + maskedValue + `"`
)

var (
	errPathNotMasked = errors.New("path not masked")
	// maskedFallback replaces payloads that cannot be masked in place; it is an object so Live Events can always parse it.
	maskedFallback = json.RawMessage(`{"masked":"` + maskedValue + `"}`)
)

func (w *worker) maskDeliveryPayload(payload json.RawMessage) json.RawMessage {
	if w.rt.reloadableConfig.disableEventDeliveryUploadMasking.Load() {
		return payload
	}

	paths, ok := w.rt.transformerFeaturesService.SecretPaths(w.rt.destType)
	var masked json.RawMessage
	var maskErr bool
	if ok {
		masked, maskErr = maskListedPaths(payload, paths)
	} else {
		masked, maskErr = maskAll(payload)
	}

	var reason string
	switch {
	case maskErr:
		reason = "mask_error"
	case !ok:
		reason = "mask_all"
	case bytes.Equal(masked, payload): // masking only ever writes maskedValue, so equal bytes mean nothing matched
		reason = "listed_noop"
	default:
		reason = "listed"
	}
	w.rt.deliveryPayloadMaskingStat(reason).Increment()
	return masked
}

// maskListedPaths replaces existing values at paths. Router-transform request arrays are returned
// as arrays after masking each request object because Live Events accepts that payload shape.
// Malformed payloads, mixed arrays, no-match non-empty arrays, and any path application error fall
// back to a non-leaking object so masking never forwards an unmodified multi-request payload.
func maskListedPaths(payload json.RawMessage, paths []string) (json.RawMessage, bool) {
	parsed := gjson.ParseBytes(payload)
	if !gjson.ValidBytes(payload) || !(parsed.IsObject() || parsed.IsArray()) {
		return maskedFallback, true
	}
	if parsed.IsArray() {
		return maskListedPathsInArray(payload, parsed, paths)
	}
	masked, err := applyListedPaths(payload, paths)
	if err != nil {
		fallback, _ := maskAll(masked)
		return fallback, true
	}
	return masked, false
}

func maskListedPathsInArray(payload json.RawMessage, requests gjson.Result, paths []string) (json.RawMessage, bool) {
	var maskedRequests [][]byte
	changed, failed := false, false
	requests.ForEach(func(_, request gjson.Result) bool {
		if !request.IsObject() {
			failed = true
			return false
		}
		masked, err := applyListedPaths(json.RawMessage(request.Raw), paths)
		if err != nil {
			failed = true
			return false
		}
		maskedRequests = append(maskedRequests, masked)
		changed = changed || string(masked) != request.Raw
		return true
	})
	switch {
	case failed:
		return maskedFallback, true
	case len(maskedRequests) == 0:
		return payload, false
	case !changed:
		return maskedFallback, true
	}
	return misc.MakeJSONArray(maskedRequests), false
}

// applyListedPaths never mutates payload: sjson allocates a new slice unless ReplaceInPlace is set.
func applyListedPaths(masked json.RawMessage, paths []string) (json.RawMessage, error) {
	var err error
	for _, path := range paths {
		if path == "endpoint" {
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
				if masked, err = setMasked(masked, parentPath+"."+strconv.Itoa(idx)); err != nil {
					return masked, err
				}
			}
			continue
		}

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
// non-object JSON are replaced wholesale with maskedFallback.
func maskAll(payload json.RawMessage) (json.RawMessage, bool) {
	if !isJSONObject(payload) {
		return maskedFallback, true
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
			rebuilt.WriteString(maskedJSON)
		}
		return true
	})
	rebuilt.WriteByte('}')
	return rebuilt.Bytes(), false
}

func isJSONObject(payload json.RawMessage) bool {
	return gjson.ValidBytes(payload) && gjson.ParseBytes(payload).IsObject()
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

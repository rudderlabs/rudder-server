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

const (
	maskedValue          = "******"
	maskedJSON           = `"` + maskedValue + `"`
	fallbackMaskedObject = `{"masked":"` + maskedValue + `"}`
)

var errPathNotMasked = errors.New("path not masked")

func (w *worker) maskDeliveryPayload(payload json.RawMessage) json.RawMessage {
	if w.rt.reloadableConfig.disableEventDeliveryUploadMasking.Load() {
		return payload
	}

	paths, ok := w.rt.transformerFeaturesService.SecretPaths(w.rt.destType)
	var masked json.RawMessage
	var maskErr, maskedListedPath bool
	if ok {
		masked, maskErr, maskedListedPath = maskListedPathsWithResult(payload, paths)
	} else {
		masked, maskErr = maskAll(payload)
	}

	var reason string
	switch {
	case maskErr:
		reason = "mask_error"
	case !ok:
		reason = "mask_all"
	case !maskedListedPath:
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
	masked, maskErr, _ := maskListedPathsWithResult(payload, paths)
	return masked, maskErr
}

func maskListedPathsWithResult(payload json.RawMessage, paths []string) (json.RawMessage, bool, bool) {
	if !gjson.ValidBytes(payload) {
		return json.RawMessage(fallbackMaskedObject), true, false
	}
	parsed := gjson.ParseBytes(payload)
	switch {
	case parsed.IsObject():
		if len(paths) == 0 {
			return payload, false, false
		}
		masked, listedPathFound, err := applyListedPaths(payload, paths)
		if err != nil {
			fallback, _ := maskAll(masked)
			return fallback, true, false
		}
		return masked, false, listedPathFound
	case parsed.IsArray():
		return maskListedPathsInArray(parsed.Array(), paths)
	default:
		return json.RawMessage(fallbackMaskedObject), true, false
	}
}

func maskListedPathsInArray(requests []gjson.Result, paths []string) (json.RawMessage, bool, bool) {
	var maskedArray bytes.Buffer
	maskedArray.WriteByte('[')
	listedPathFound := false
	for i, request := range requests {
		if !request.IsObject() {
			return json.RawMessage(fallbackMaskedObject), true, false
		}
		masked, requestPathFound, err := applyListedPaths(json.RawMessage(request.Raw), paths)
		if err != nil {
			return json.RawMessage(fallbackMaskedObject), true, false
		}
		if i > 0 {
			maskedArray.WriteByte(',')
		}
		maskedArray.Write(masked)
		listedPathFound = listedPathFound || requestPathFound
	}
	maskedArray.WriteByte(']')
	if len(requests) > 0 && !listedPathFound {
		return json.RawMessage(fallbackMaskedObject), true, false
	}
	return maskedArray.Bytes(), false, listedPathFound
}

// applyListedPaths never mutates payload: sjson allocates a new slice unless ReplaceInPlace is set.
func applyListedPaths(masked json.RawMessage, paths []string) (json.RawMessage, bool, error) {
	var err error
	listedPathFound := false
	for _, path := range paths {
		if path == "endpoint" {
			continue
		}
		if path == "" {
			return masked, listedPathFound, errPathNotMasked
		}
		if !gjson.GetBytes(masked, path).Exists() {
			continue
		}

		if parentPath, ok := terminalArrayWildcardParent(path); ok {
			array := gjson.GetBytes(masked, parentPath)
			if !array.IsArray() {
				return masked, listedPathFound, errPathNotMasked
			}
			for idx := range array.Array() {
				if masked, err = setMasked(masked, parentPath+"."+strconv.Itoa(idx)); err != nil {
					return masked, listedPathFound, err
				}
				listedPathFound = true
			}
			continue
		}

		if masked, err = setMasked(masked, path); err != nil {
			return masked, listedPathFound, err
		}
		listedPathFound = true
	}
	return masked, listedPathFound, nil
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
// non-object JSON are replaced wholesale with a minimal object so Live Events can always parse it.
func maskAll(payload json.RawMessage) (json.RawMessage, bool) {
	if !isJSONObject(payload) {
		return json.RawMessage(fallbackMaskedObject), true
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

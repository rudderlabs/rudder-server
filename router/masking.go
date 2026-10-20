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
	masked, reason := maskForDelivery(payload, paths, ok)
	w.rt.deliveryPayloadMaskingStat(reason).Increment()
	return masked
}

// maskForDelivery masks a delivery payload and returns it with a stats reason. Router-transform
// request arrays stay arrays because Live Events accepts that payload shape.
func maskForDelivery(payload json.RawMessage, paths []string, ok bool) (json.RawMessage, string) {
	var masked json.RawMessage
	var reason string
	if ok && gjson.ValidBytes(payload) && gjson.ParseBytes(payload).IsArray() {
		masked, reason = maskSecretsInArray(payload, paths)
	} else {
		masked, reason = maskSecrets(payload, paths, ok)
	}
	switch reason {
	case "no_paths", "not_object", "partial":
		// fail closed: an unknown destination or an unmaskable path must never forward a raw credential
		var maskErr bool
		masked, maskErr = maskAll(payload)
		if !ok && !maskErr {
			reason = "mask_all"
		} else {
			reason = "mask_error"
		}
	}
	return masked, reason
}

// maskSecretsInArray masks each request object independently; a request that cannot be
// fully masked fails the whole array.
func maskSecretsInArray(payload json.RawMessage, paths []string) (json.RawMessage, string) {
	var maskedRequests [][]byte
	changed := false
	for _, request := range gjson.ParseBytes(payload).Array() {
		masked, reason := maskSecrets(json.RawMessage(request.Raw), paths, true)
		switch reason {
		case "not_object", "partial":
			return payload, reason
		case "listed":
			changed = true
		}
		maskedRequests = append(maskedRequests, masked)
	}
	if !changed { // return the original bytes so the reason is listed_noop, not a re-serialized array
		return payload, "listed_noop"
	}
	return misc.MakeJSONArray(maskedRequests), "listed"
}

// maskReportingSampleEvent leaves the sample untouched where it cannot mask precisely:
// a blanket-masked sample destroys the error-triage value the sample exists for.
func (rt *Handle) maskReportingSampleEvent(payload json.RawMessage) json.RawMessage {
	if !rt.reloadableConfig.enableReportingSampleEventMasking.Load() {
		return payload
	}
	paths, ok := rt.transformerFeaturesService.SecretPaths(rt.destType)
	masked, reason := maskSecrets(payload, paths, ok)
	rt.reportingSampleEventMaskingStat(reason).Increment()
	return masked
}

// maskSecrets masks every listed path it can; paths and ok come from
// transformerFeaturesService.SecretPaths. Returns the masked payload and a stats reason.
func maskSecrets(payload json.RawMessage, paths []string, ok bool) (json.RawMessage, string) {
	if !ok {
		return payload, "no_paths"
	}
	if len(paths) == 0 {
		return payload, "listed_noop"
	}
	if !isJSONObject(payload) {
		return payload, "not_object"
	}
	masked, failed := applyPaths(payload, paths)
	switch {
	case failed:
		return masked, "partial"
	case bytes.Equal(masked, payload): // masking only ever writes maskedValue, so equal bytes mean nothing matched
		return masked, "listed_noop"
	default:
		return masked, "listed"
	}
}

// applyPaths masks every listed path that exists. A path that is absent is skipped and a
// path that fails to apply never contributes its partial write.
func applyPaths(payload json.RawMessage, paths []string) (_ json.RawMessage, failed bool) {
	masked := payload
	for _, path := range paths {
		if path == "endpoint" {
			continue
		}
		if path == "" {
			failed = true
			continue
		}
		if !gjson.GetBytes(masked, path).Exists() {
			continue
		}
		if parentPath, ok := terminalArrayWildcardParent(path); ok {
			array := gjson.GetBytes(masked, parentPath)
			if !array.IsArray() {
				failed = true
				continue
			}
			for idx := range array.Array() {
				next, err := setMasked(masked, parentPath+"."+strconv.Itoa(idx))
				if err != nil {
					failed = true
					continue
				}
				masked = next
			}
			continue
		}
		next, err := setMasked(masked, path)
		if err != nil {
			failed = true
			continue
		}
		masked = next
	}
	return masked, failed
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
	if !gjson.ValidBytes(payload) {
		return false
	}
	trimmed := bytes.TrimSpace(payload)
	return len(trimmed) > 0 && trimmed[0] == '{'
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

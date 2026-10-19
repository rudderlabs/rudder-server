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
	maskedValue = "******"
	maskedJSON  = `"` + maskedValue + `"`
)

// maskMode selects what happens when the transformer publishes no usable secret paths
// for a destination, or when a listed path cannot be applied.
type maskMode int

const (
	// maskModeFallbackAll masks every top-level field except endpoint. Live events use
	// this: an unknown destination must never forward a raw credential.
	maskModeFallbackAll maskMode = iota
	// maskModeSkip leaves the payload untouched. Reporting sample events use this: a
	// blanket-masked sample destroys the error-triage value the sample exists for.
	maskModeSkip
)

func (w *worker) maskDeliveryPayload(payload json.RawMessage) json.RawMessage {
	if w.rt.reloadableConfig.disableEventDeliveryUploadMasking.Load() {
		return payload
	}
	paths, ok := w.rt.transformerFeaturesService.SecretPaths(w.rt.destType)
	masked, reason := maskSecrets(payload, paths, ok, maskModeFallbackAll)
	w.rt.deliveryPayloadMaskingStat(reason).Increment()
	return masked
}

func (w *worker) maskReportingSampleEvent(payload json.RawMessage) json.RawMessage {
	if !w.rt.reloadableConfig.enableReportingSampleEventMasking.Load() {
		return payload
	}
	paths, ok := w.rt.transformerFeaturesService.SecretPaths(w.rt.destType)
	masked, reason := maskSecrets(payload, paths, ok, maskModeSkip)
	w.rt.reportingSampleEventMaskingStat(reason).Increment()
	return masked
}

// maskSecrets masks paths in payload; paths and ok come from
// transformerFeaturesService.SecretPaths. Returns the masked payload and a stats reason.
func maskSecrets(payload json.RawMessage, paths []string, ok bool, mode maskMode) (json.RawMessage, string) {
	if !ok {
		if mode == maskModeSkip {
			return payload, "no_paths"
		}
		masked, maskErr := maskAll(payload)
		if maskErr {
			return masked, "mask_error"
		}
		return masked, "mask_all"
	}
	if len(paths) == 0 {
		return payload, "listed_noop"
	}
	if !isJSONObject(payload) {
		if mode == maskModeSkip {
			return payload, "not_object"
		}
		masked, _ := maskAll(payload)
		return masked, "mask_error"
	}

	masked, applied, failed := applyPaths(payload, paths)
	switch {
	case failed && mode == maskModeFallbackAll:
		fallback, _ := maskAll(payload)
		return fallback, "mask_error"
	case failed:
		return masked, "partial"
	case applied:
		return masked, "listed"
	default:
		return masked, "listed_noop"
	}
}

// applyPaths masks every listed path that exists. A path that is absent is skipped and a
// path that fails to apply never contributes its partial write.
func applyPaths(payload json.RawMessage, paths []string) (_ json.RawMessage, applied, failed bool) {
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
				masked, applied = next, true
			}
			continue
		}
		next, err := setMasked(masked, path)
		if err != nil {
			failed = true
			continue
		}
		masked, applied = next, true
	}
	return masked, applied, failed
}

func setMasked(payload json.RawMessage, path string) (json.RawMessage, error) {
	masked, err := sjson.SetBytes(payload, path, maskedValue)
	if err != nil {
		return masked, err
	}
	if !resultMasked(gjson.GetBytes(masked, path)) {
		return masked, errors.New("path not masked")
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
		return json.RawMessage(maskedJSON), true
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

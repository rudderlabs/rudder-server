package warehouseutils

import (
	"encoding/json"

	patchapply "github.com/evanphx/json-patch/v5"
	patchgen "github.com/mattbaird/jsonpatch"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
)

// GenerateJSONPatch generates a JSON patch (RFC 6902) that transforms original into modified.
// Returns the patch as json.RawMessage.
func GenerateJSONPatch(original, modified json.RawMessage) (json.RawMessage, error) {
	patch, err := patchgen.CreatePatch(original, modified)
	if err != nil {
		return nil, err
	}
	patchBytes, err := jsonrs.Marshal(patch)
	if err != nil {
		return nil, err
	}
	return patchBytes, nil
}

// ApplyPatchToJSON applies a JSON patch (RFC 6902) to a document and returns the result as json.RawMessage.
func ApplyPatchToJSON(original, patch json.RawMessage) (json.RawMessage, error) {
	patchObj, err := patchapply.DecodePatch(patch)
	if err != nil {
		return nil, err
	}
	result, err := patchObj.Apply(original)
	if err != nil {
		return nil, err
	}
	return result, nil
}

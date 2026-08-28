package backendconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	kitjsonparser "github.com/rudderlabs/rudder-go-kit/jsonparser"
)

// v2ConfigVersion is the version the v2 namespace config endpoint stamps on its response.
const v2ConfigVersion = 2

// Pinning the gjson backed implementation rather than taking the default one: workspace config bodies are
// the largest JSON this process scans, and gjson goes through them at roughly twice the rate, measured from 16KB up to
// 100MB with the field last in the object, which is where the control plane puts it.
var jsonparser = kitjsonparser.NewWithLibrary(kitjsonparser.TidwallLib)

// v2NamespaceConfig is the response of the v2 namespace config endpoint:
//
//	GET /configuration/v2/namespaces/{namespace}?secrets=embed
//
// The definitions that v1 embeds in every source and destination are hoisted here into three
// namespace-global catalogues sent once, and each workspace references them by name.
type v2NamespaceConfig struct {
	Version int `json:"version"`
	v2Catalogues
	Workspaces v2Workspaces `json:"workspaces"`
}

// v2Workspaces is the only updateable list of the response: a workspace that did not change since
// updatedAfter arrives as null, and the key set is the namespace's membership.
type v2Workspaces map[string]*v2Workspace

// v2Catalogues are the namespace-global definition catalogues, keyed by definition name.
//
// An entry is its v1 definition plus UpdatedAt, which the v1 types do not carry and which feeds
// the generation key of the transform memo. Whatever else is on the wire (configSchema, uiConfig,
// responseRules, ...) stays unparsed: a v1 ConfigT has nowhere to put it, so it would be decoded
// and held on every poll without ever reaching a consumer.
type v2Catalogues struct {
	SourceDefinitions      v2SourceDefinitions      `json:"sourceDefinitions"`
	DestinationDefinitions v2DestinationDefinitions `json:"destinationDefinitions"`
	AccountDefinitions     v2AccountDefinitions     `json:"accountDefinitions"`
}

type (
	v2SourceDefinitions      map[string]v2SourceDefinition
	v2DestinationDefinitions map[string]v2DestinationDefinition
	v2AccountDefinitions     map[string]v2AccountDefinition
)

// v2DefinitionMeta is what every catalogue entry adds to its v1 definition: when it last changed,
// which the v1 types do not carry and the generation key is folded from.
type v2DefinitionMeta struct {
	UpdatedAt time.Time `json:"updatedAt"`
}

func (m v2DefinitionMeta) updatedAt() time.Time { return m.UpdatedAt }

// v2SourceDefinition is an entry of the sourceDefinitions catalogue.
//
// Note the embedded Name is not on the wire: source and destination definitions are named by their
// catalogue key alone. Reach for toV1 rather than the embedded value, or the name is empty.
type v2SourceDefinition struct {
	SourceDefinitionT
	v2DefinitionMeta
}

// toV1 returns the definition as v1 spells it, named after the key it is catalogued under.
func (d v2SourceDefinition) toV1(name string) SourceDefinitionT {
	definition := d.SourceDefinitionT
	definition.Name = name
	return definition
}

// v2DestinationDefinition is an entry of the destinationDefinitions catalogue.
type v2DestinationDefinition struct {
	DestinationDefinitionT
	v2DefinitionMeta
}

// toV1 returns the definition as v1 spells it, named after the key it is catalogued under.
func (d v2DestinationDefinition) toV1(name string) DestinationDefinitionT {
	definition := d.DestinationDefinitionT
	definition.Name = name
	return definition
}

// v2AccountDefinition is an entry of the accountDefinitions catalogue. Unlike the other two,
// account definitions do carry their own name, which the control plane keeps equal to the key.
type v2AccountDefinition struct {
	AccountDefinition
	v2DefinitionMeta
}

// toV1 returns the definition as v1 spells it, named after the key it is catalogued under.
func (d v2AccountDefinition) toV1(name string) AccountDefinition {
	definition := d.AccountDefinition
	definition.Name = name
	return definition
}

// v2Workspace is one workspace of the response.
//
// The workspace body is deliberately left untyped: the resolver caches the raw bytes and hands
// them to the mapper, so the full workspace schema belongs to the mapper alone. Only updatedAt is
// read here, and it is load bearing - it drives the updatedAfter of the next poll and the memo
// that lets an unchanged workspace skip the transform.
type v2Workspace struct {
	UpdatedAt time.Time
	Raw       json.RawMessage
}

func (w *v2Workspace) UnmarshalJSON(data []byte) error {
	// the body is scanned for the one field we need rather than decoded: the decoder that handed
	// us these bytes has already validated them, and decoding a single field out of a workspace
	// this size costs more than the scan does
	updatedAt, err := jsonparser.GetString(data, "updatedAt")
	switch {
	case errors.Is(err, kitjsonparser.ErrKeyNotFound): // lenient
	case err != nil:
		return fmt.Errorf("reading workspace updatedAt: %w", err)
	default:
		parsed, err := time.Parse(time.RFC3339, updatedAt)
		if err != nil {
			return fmt.Errorf("parsing workspace updatedAt: %w", err)
		}
		w.UpdatedAt = parsed
	}
	// sonnet, the default jsonrs implementation, hands back a slice of the response buffer rather
	// than a copy, as does std: a cached workspace would otherwise pin the whole payload for as
	// long as it is cached, and change under us if that buffer is ever reused
	w.Raw = bytes.Clone(data)
	return nil
}

// mapper joins one workspace's raw v2 blob with the namespace-global catalogues to produce a v1
// ConfigT. The post-transform steps (ApplyReplaySources, processAccountAssociations,
// ProcessDestinationsInSources) stay with the caller.
type mapper interface {
	Map(workspaceID string, raw json.RawMessage, catalogues v2Catalogues) (ConfigT, error)
}

package backendconfig

import (
	"iter"
	"time"

	"github.com/rudderlabs/rudder-cp-sdk/diff"
)

// The v2 response is merged into the cached one by the SDK's differ, which needs the response to
// describe which of its lists carry incremental updates and which are replaced wholesale.
//
// Workspaces are updateable: a poll with updatedAfter returns null for every workspace that did
// not change, and the differ fills those back in from the cache. Definitions are not: the control
// plane always sends the catalogues in full, so they are replaced on every poll - which is also
// the only way a deleted definition ever leaves the cache.
var (
	_ diff.UpdateableObject[string]                       = &v2NamespaceConfig{}
	_ diff.UpdateableList[string, diff.UpdateableElement] = &v2Workspaces{}
	_ diff.UpdateableElement                              = &v2Workspace{}
	_ diff.NonUpdateablesList[string, any]                = &v2SourceDefinitions{}
	_ diff.NonUpdateablesList[string, any]                = &v2DestinationDefinitions{}
	_ diff.NonUpdateablesList[string, any]                = &v2AccountDefinitions{}
)

func (c *v2NamespaceConfig) Updateables() iter.Seq[diff.UpdateableList[string, diff.UpdateableElement]] {
	return func(yield func(diff.UpdateableList[string, diff.UpdateableElement]) bool) {
		yield(&c.Workspaces)
	}
}

func (c *v2NamespaceConfig) NonUpdateables() iter.Seq[diff.NonUpdateablesList[string, any]] {
	return func(yield func(diff.NonUpdateablesList[string, any]) bool) {
		if !yield(&c.SourceDefinitions) {
			return
		}
		if !yield(&c.DestinationDefinitions) {
			return
		}
		yield(&c.AccountDefinitions)
	}
}

func (w *v2Workspace) GetUpdatedAt() time.Time { return w.UpdatedAt }

func (w *v2Workspace) IsNil() bool { return w == nil }

func (ws *v2Workspaces) Type() string { return "Workspaces" }

func (ws *v2Workspaces) Length() int { return len(*ws) }

func (ws *v2Workspaces) Reset() { *ws = make(v2Workspaces) }

func (ws *v2Workspaces) List() iter.Seq2[string, diff.UpdateableElement] {
	return func(yield func(string, diff.UpdateableElement) bool) {
		for id, workspace := range *ws {
			if !yield(id, workspace) {
				return
			}
		}
	}
}

func (ws *v2Workspaces) GetElementByKey(id string) (diff.UpdateableElement, bool) {
	workspace, ok := (*ws)[id]
	return workspace, ok
}

func (ws *v2Workspaces) SetElementByKey(id string, object diff.UpdateableElement) {
	if *ws == nil {
		*ws = make(v2Workspaces)
	}
	(*ws)[id] = object.(*v2Workspace)
}

func (d *v2SourceDefinitions) Type() string { return "SourceDefinitions" }

func (d *v2SourceDefinitions) Reset() { *d = make(v2SourceDefinitions) }

func (d *v2SourceDefinitions) List() iter.Seq2[string, any] { return listDefinitions(*d) }

func (d *v2SourceDefinitions) SetElementByKey(name string, object any) {
	setDefinition(d, name, object)
}

func (d *v2DestinationDefinitions) Type() string { return "DestinationDefinitions" }

func (d *v2DestinationDefinitions) Reset() { *d = make(v2DestinationDefinitions) }

func (d *v2DestinationDefinitions) List() iter.Seq2[string, any] { return listDefinitions(*d) }

func (d *v2DestinationDefinitions) SetElementByKey(name string, object any) {
	setDefinition(d, name, object)
}

func (d *v2AccountDefinitions) Type() string { return "AccountDefinitions" }

func (d *v2AccountDefinitions) Reset() { *d = make(v2AccountDefinitions) }

func (d *v2AccountDefinitions) List() iter.Seq2[string, any] { return listDefinitions(*d) }

func (d *v2AccountDefinitions) SetElementByKey(name string, object any) {
	setDefinition(d, name, object)
}

// listDefinitions adapts a catalogue to the iterator the differ replaces it through.
func listDefinitions[M ~map[string]T, T any](definitions M) iter.Seq2[string, any] {
	return func(yield func(string, any) bool) {
		for name, definition := range definitions {
			if !yield(name, definition) {
				return
			}
		}
	}
}

// setDefinition is the inverse of listDefinitions. The differ only ever hands back values it took
// out of a catalogue of the same type, so the assertion cannot fail.
func setDefinition[M ~map[string]T, T any](definitions *M, name string, object any) {
	if *definitions == nil {
		*definitions = make(M)
	}
	(*definitions)[name] = object.(T)
}

package features_test

import (
	"testing"

	"github.com/rudderlabs/rudder-server/services/features"
	"github.com/stretchr/testify/require"
)

func TestRegistry(t *testing.T) {
	reg := features.Registry{}
	reg.Register("test", []string{"feature1", "feature2"})
	reg.Register("test2", []string{"feature3", "feature4"})

	type component struct {
		name     string
		features []string
	}
	cc := []component{}

	reg.Each(func(name string, features []string) {
		cc = append(cc, component{name: name, features: features})
	})

	require.Equal(t, []component{
		{name: "test", features: []string{"feature1", "feature2"}},
		{name: "test2", features: []string{"feature3", "feature4"}},
	}, cc)
}

package bqstreamv2

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStreamWriterKey(t *testing.T) {
	cfg := destConfig{ProjectID: "my-project", Namespace: "my_namespace", Credentials: "creds"}
	require.Equal(t, "my-project:my_namespace:pages", streamWriterKey(cfg, "pages"))
}

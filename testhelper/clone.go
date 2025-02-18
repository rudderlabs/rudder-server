package testhelper

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/jsonrs"
)

func Clone[T any](t testing.TB, v T) T {
	t.Helper()

	buf, err := jsonrs.Marshal(v)
	require.NoError(t, err)

	var clone T
	require.NoError(t, jsonrs.Unmarshal(buf, &clone))

	return clone
}

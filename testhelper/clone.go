package testhelper

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func Clone[T any](t testing.TB, v T) T {
	t.Helper()

	buf, err := json.Marshal(v)
	require.NoError(t, err)

	var clone T
	require.NoError(t, json.Unmarshal(buf, &clone))

	return clone
}

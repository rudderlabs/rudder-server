package source_test

import (
	"fmt"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/source"
	"github.com/stretchr/testify/require"
)

func TestCustomTime(t *testing.T) {

	ct := source.CustomTime{}

	err := ct.UnmarshalJSON([]byte("01-02-2006 15:04:05"))
	require.NoError(t, err)

	fmt.Println(ct)
	require.Equal(t, "2006-01-02 15:04:05 +0000 UTC", ct.String())
}

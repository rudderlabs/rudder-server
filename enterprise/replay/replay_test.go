package replay

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestParseFileKey(t *testing.T) {
	workspaceID := "workspaceID"
	file := fmt.Sprintf("%v.json.gz", fmt.Sprintf("%v.%v.%v.%v.%v", time.Now().Unix(), "1", fmt.Sprintf("%v-%v", 20, 32), uuid.New().String(), workspaceID))

	path := filepath.Join("rudder-proc-err-logs", time.Now().Format("01-02-2006"), file)
	df, err := ParseFileKey(path)
	require.NoError(t, err)

	require.Equal(t, df.InstanceID, 1)
	require.Equal(t, df.MinJobID, 20)
	require.Equal(t, df.MaxJobID, 32)
}

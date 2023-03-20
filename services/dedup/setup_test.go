package dedup_test

import (
	"testing"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/services/dedup"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/stretchr/testify/require"
)

func Test_GetInstance(t *testing.T) {
	config.Reset()
	logger.Reset()
	misc.Init()
	dedup.Init()

	i := dedup.GetInstance(nil)

	i2 := dedup.GetInstance(nil)

	require.Equal(t, i, i2)
}

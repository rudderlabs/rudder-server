package misc_test

import (
	"testing"

	uuid "github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/stretchr/testify/require"

	gluuid "github.com/google/uuid"
)

var uuidGOOGLE gluuid.UUID
var uuidGOFRS uuid.UUID

func init() {
	gluuid.EnableRandPool()
}

func Test_fastUUID(t *testing.T) {
	t.Run("test google conversion gofrs", func(t *testing.T) {
		uuidGOOGLE = gluuid.New()
		b, _ := uuidGOOGLE.MarshalBinary()
		uuidGOFRS = uuid.FromBytesOrNil(b)
		require.Equal(t, uuidGOOGLE.String(), uuidGOFRS.String())
	})
}

func Benchmark_GOOGLE_UUID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		uuidGOOGLE = gluuid.New()
	}
}

func Benchmark_GOOGLE_UUID_STR_GOFRS(b *testing.B) {
	for i := 0; i < b.N; i++ {
		uuidGOFRS = uuid.FromStringOrNil(gluuid.New().String())
	}
}

func Benchmark_GOOGLE_UUID_BIN_GOFRS(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b, _ := gluuid.New().MarshalBinary()
		uuidGOFRS = uuid.FromBytesOrNil(b)
	}
}

func Benchmark_FAST_UUID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		uuidGOFRS = misc.FastUUID()
	}
}

func Benchmark_GOFRS_UUID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		uuidGOFRS = uuid.Must(uuid.NewV4())
	}
}

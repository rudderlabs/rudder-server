// This package is testing and comparing the performance of the following UUID packages:
//
// - github.com/gofrs/uuid
// - github.com/google/uuid

package misc_test

import (
	"crypto/md5"
	"testing"

	uuid "github.com/gofrs/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/utils/misc"

	gluuid "github.com/google/uuid"
)

var (
	uuidGOOGLE gluuid.UUID
	uuidGOFRS  uuid.UUID
)

func init() {
	gluuid.EnableRandPool()
}

func GetMD5UUIDOld(str string) (uuid.UUID, error) {
	md5Sum := md5.Sum([]byte(str))
	u, err := uuid.FromBytes(md5Sum[:])

	u.SetVersion(uuid.V4)
	u.SetVariant(uuid.VariantRFC4122)
	return u, err
}

func FuzzGetMD5UUID(f *testing.F) {
	f.Add("hello")
	f.Add("")
	f.Add(gluuid.New().String())

	f.Fuzz(func(t *testing.T, a string) {
		t.Log(a)

		gMD5, err := misc.GetMD5UUID(a)
		require.NoError(t, err)

		oldMD5, err := GetMD5UUIDOld(a)
		require.NoError(t, err)

		require.Equal(t, oldMD5.String(), gMD5.String())
	})
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
		uuidGOOGLE = misc.FastUUID()
	}
}

func Benchmark_GOFRS_UUID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		uuidGOFRS = uuid.Must(uuid.NewV4())
	}
}

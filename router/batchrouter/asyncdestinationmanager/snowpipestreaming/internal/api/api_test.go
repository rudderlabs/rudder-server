package api

import (
	"errors"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
)

func TestMustReadAll(t *testing.T) {
	t.Run("ReadAll", func(t *testing.T) {
		r := strings.NewReader("hello")
		data := mustRead(r)
		require.Equal(t, []byte("hello"), data)
	})
	t.Run("ReadAll error", func(t *testing.T) {
		r := iotest.ErrReader(errors.New("error"))
		data := mustRead(r)
		require.Empty(t, data)
	})
}

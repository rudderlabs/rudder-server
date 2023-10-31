package misc_test

import (
	"fmt"
	"testing"
	"unicode"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/unicode/rangetable"
)

func BenchmarkMessageID(b *testing.B) {
	dirtyMessageID := "\u0000 Test foo_bar-baz \u034F 123-222 "
	properMessageID := "123e4567-e89b-12d3-a456-426614174000"

	b.Run("in-place for loop - dirty", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			sanitizeMessageIDForLoop(dirtyMessageID)
		}
	})

	b.Run("in-place for loop - proper", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			sanitizeMessageIDForLoop(properMessageID)
		}
	})

	b.Run("strings map - dirty", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			misc.SanitizeUnicode(dirtyMessageID)
		}
	})

	b.Run("strings map - proper", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			misc.SanitizeUnicode(properMessageID)
		}
	})

}

var invisibleRangeTable *unicode.RangeTable

func init() {
	invisibleRangeTable = rangetable.New(misc.InvisibleRunes...)
}

// incorrect implementation of sanitizeMessageID, but used for benchmarking
func sanitizeMessageIDForLoop(messageID string) string {
	for i, r := range messageID {
		if unicode.IsPrint(r) {
			continue
		}
		if !unicode.Is(invisibleRangeTable, r) {
			continue
		}

		messageID = messageID[:i] + messageID[i+1:]
	}
	return messageID

}

func TestSanitizeMessageID(t *testing.T) {
	testcases := []struct {
		in  string
		out string
	}{
		{"\u0000 Test \u0000foo_bar-baz 123-222 \u0000", " Test foo_bar-baz 123-222 "},
		{"\u0000", ""},
		{"\u0000 ", " "},
		{"\u0000 \u0000", " "},
		{"\u00A0\t\n\r\u034F", ""},
		{"τυχαίο;", "τυχαίο;"},
	}

	for _, tc := range testcases {
		cleanMessageID := misc.SanitizeUnicode(tc.in)
		require.Equal(t, tc.out, cleanMessageID, fmt.Sprintf("%#v -> %#v", tc.in, tc.out))
	}

	for _, r := range misc.InvisibleRunes {
		cleanMessageID := misc.SanitizeUnicode(string(r))
		require.Empty(t, cleanMessageID, fmt.Sprintf("%U", r))

	}
}

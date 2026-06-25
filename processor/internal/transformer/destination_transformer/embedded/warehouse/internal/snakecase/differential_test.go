package snakecase

import (
	"bufio"
	"fmt"
	"os"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/warehouse/internal/snakecase/internal/oracle"
)

// This file is the safety harness for replacing the regexp2-backed
// implementation of ToSnakeCase / ToSnakeCaseWithNumbers.
//
// The package `internal/oracle` holds a FROZEN, verbatim copy of the original
// regexp2 implementation. Every test here asserts that the production functions
// produce byte-for-byte identical output to the oracle. As long as these pass,
// any reimplementation of snakecase is behavior-preserving.
//
// Run the full harness with:
//
//	go test ./...snakecase/ -run Differential
//	go test ./...snakecase/ -run xxx -fuzz FuzzSnakeCaseDifferential -fuzztime=60s
//	go test ./...snakecase/ -run xxx -fuzz FuzzSnakeCaseNumbersDifferential -fuzztime=60s

// differentialCorpus is a curated set of inputs spanning every behavior the
// original implementation is known to exhibit, plus realistic warehouse column
// names. It is intentionally exhaustive on the ASCII shapes that dominate
// production traffic, with a unicode/emoji tail to guard the hard cases.
var differentialCorpus = []string{
	// trivial
	"", " ", "  ", "\t", "\n", "a", "A", "_", "__", "-", "--",

	// realistic warehouse column names (camelCase / snake / dotted-flattened)
	"messageId", "anonymousId", "userId", "sentAt", "timestamp", "receivedAt",
	"originalTimestamp", "channel", "request_ip", "review_id", "product_id",
	"rating", "review_body", "context_traits_name", "context_traits_email",
	"context_traits_logins", "context_ip", "userProperties_rating",
	"userProperties_review_body", "alreadySnakeCasedColumnName",
	"someVeryLongColumnNameThatExceedsTheTypicalLengthOfAColumnNameUsedInWarehouse",

	// camel / pascal / caps boundaries
	"fooBar", "FooBar", "foo Bar", "Foo Bar", "FOO BAR", "XMLHttp", "XmlHTTP",
	"XmlHttp", "XMLHttpRequest", "isISO8601", "LETTERSAeiouAreVowels",
	"aeiouAreVowels",

	// separators
	"--foo-bar--", "__foo_bar__", "foo.bar.baz", "foo/bar\\baz", "foo:bar;baz",
	"foo  bar   baz", "  leading", "trailing  ", "a-b_c.d e",

	// numbers (the two variants differ here)
	"12ft", "enable 6h format", "enable 24H format", "tooLegit2Quit",
	"walk500Miles", "xhr2Request", "isISO8601Date", "v2Api", "api2V3",
	"123", "123abc", "abc123", "123ABC", "ABC123", "a1b2c3", "1st2nd3rd",

	// ordinals
	"1st", "2nd", "3rd", "4th", "1ST", "2ND", "3RD", "4TH", "21st", "102nd",
	"1stPlace", "the2ndItem",

	// contractions
	"a b'd c", "a b'll c", "a b'm c", "a b're c", "a b's c", "a b't c",
	"a b've c", "can't", "won't", "I'm", "they're",
	"a b’d c", "a b’ll c", "don’t",

	// diacritics / latin supplement / math operators
	"æiouAreVowels", "LETTERSÆiouAreVowels", "æiou2Consonants", "naïveCafé",
	"Über", string('\xd7'), string('\xf7'), string('\xb1'), "a×b", "a÷b",

	// unicode words / combining marks / emoji / zwj
	"café", "é", "smile😀face", "👍🏽", "👨‍👩‍👧‍👦",
	"🇺🇸flag", "❤️love", "a😀b😀c",

	// mixed adversarial
	"__123abc__XYZ789__", "foo'sBarAnd2nd", "HTTPResponse2XMLParser",
	"_leadingUnderscore", "trailingUnderscore_", "ALLCAPS", "snake_case_already",
}

func TestDifferentialCorpus(t *testing.T) {
	for _, in := range differentialCorpus {
		t.Run(fmt.Sprintf("%q", in), func(t *testing.T) {
			require.Equal(t, oracle.ToSnakeCase(in), ToSnakeCase(in),
				"ToSnakeCase diverged from oracle for input %q", in)
			require.Equal(t, oracle.ToSnakeCaseWithNumbers(in), ToSnakeCaseWithNumbers(in),
				"ToSnakeCaseWithNumbers diverged from oracle for input %q", in)
		})
	}
}

// TestDifferentialGeneratedASCII brute-forces short ASCII strings, which is the
// input class that dominates warehouse column names and the one a fast-path
// reimplementation is most likely to get subtly wrong.
func TestDifferentialGeneratedASCII(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping exhaustive ASCII sweep in -short mode")
	}
	// A small but high-signal alphabet covering case boundaries (multiple
	// distinct upper/lower letters so multi-uppercase runs like XMLHttp are
	// exercised), digits, separators, apostrophes and underscores.
	alphabet := []byte("aAbB0_- .'")
	buf := make([]byte, 0, 5)
	var rec func(depth int)
	rec = func(depth int) {
		s := string(buf)
		if got, want := ToSnakeCase(s), oracle.ToSnakeCase(s); got != want {
			t.Fatalf("ToSnakeCase(%q) = %q, oracle = %q", s, got, want)
		}
		if got, want := ToSnakeCaseWithNumbers(s), oracle.ToSnakeCaseWithNumbers(s); got != want {
			t.Fatalf("ToSnakeCaseWithNumbers(%q) = %q, oracle = %q", s, got, want)
		}
		if depth == 0 {
			return
		}
		for _, c := range alphabet {
			buf = append(buf, c)
			rec(depth - 1)
			buf = buf[:len(buf)-1]
		}
	}
	rec(5) // all strings up to length 5 over the alphabet
}

// TestDifferentialOrdinals exhaustively sweeps the digit/letter shapes that
// drive ordinal handling (1st/2nd/3rd/Nth and the 11/12/13 exceptions), which
// the general ASCII sweep's alphabet does not cover. The alphabet mixes digits
// with the exact suffix letters in both cases plus a trailing separator/letter.
func TestDifferentialOrdinals(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping ordinal sweep in -short mode")
	}
	alphabet := []byte("0123sStTnNdDrRhH_ x")
	buf := make([]byte, 0, 4)
	var rec func(depth int)
	rec = func(depth int) {
		s := string(buf)
		if got, want := ToSnakeCase(s), oracle.ToSnakeCase(s); got != want {
			t.Fatalf("ToSnakeCase(%q) = %q, oracle = %q", s, got, want)
		}
		if got, want := ToSnakeCaseWithNumbers(s), oracle.ToSnakeCaseWithNumbers(s); got != want {
			t.Fatalf("ToSnakeCaseWithNumbers(%q) = %q, oracle = %q", s, got, want)
		}
		if depth == 0 {
			return
		}
		for _, c := range alphabet {
			buf = append(buf, c)
			rec(depth - 1)
			buf = buf[:len(buf)-1]
		}
	}
	rec(4)
}

func FuzzSnakeCaseDifferential(f *testing.F) {
	for _, s := range differentialCorpus {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if !utf8.ValidString(s) {
			// regexp2 operates on runes; invalid UTF-8 is out of scope for the
			// production path (column names are always valid UTF-8).
			return
		}
		require.Equal(t, oracle.ToSnakeCase(s), ToSnakeCase(s))
	})
}

func FuzzSnakeCaseNumbersDifferential(f *testing.F) {
	for _, s := range differentialCorpus {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if !utf8.ValidString(s) {
			return
		}
		require.Equal(t, oracle.ToSnakeCaseWithNumbers(s), ToSnakeCaseWithNumbers(s))
	})
}

// loadExternalCorpus lets you point the harness at a file of real production
// column names (one per line) via SNAKECASE_CORPUS_FILE, so the differential
// check can run against the actual cardinality seen in MT deployments.
func loadExternalCorpus(tb testing.TB) []string {
	path := os.Getenv("SNAKECASE_CORPUS_FILE")
	if path == "" {
		tb.Skip("set SNAKECASE_CORPUS_FILE to a newline-delimited corpus to run this test")
	}
	file, err := os.Open(path)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = file.Close() })

	var out []string
	sc := bufio.NewScanner(file)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		out = append(out, sc.Text())
	}
	require.NoError(tb, sc.Err())
	return out
}

func TestDifferentialExternalCorpus(t *testing.T) {
	for _, in := range loadExternalCorpus(t) {
		require.Equalf(t, oracle.ToSnakeCase(in), ToSnakeCase(in), "ToSnakeCase %q", in)
		require.Equalf(t, oracle.ToSnakeCaseWithNumbers(in), ToSnakeCaseWithNumbers(in), "ToSnakeCaseWithNumbers %q", in)
	}
}

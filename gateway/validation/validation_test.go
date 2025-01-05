package validation

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJobsDB_SanitizeJSON(t *testing.T) {
	ch := func(n int) string {
		return strings.Repeat("�", n)
	}
	toValidUTF8Tests := []struct {
		in  string
		out string
		err error
	}{
		{`\u0000`, "", nil},
		{`\u0000☺\u0000b☺`, "☺b☺", nil},
		// NOTE: we are not handling the following:
		// {"\u0000", ""},
		// {"\u0000☺\u0000b☺", "☺b☺"},

		{"", "", nil},
		{"abc", "abc", nil},
		{"\uFDDD", "\uFDDD", nil},
		{"a\xffb", "a" + ch(1) + "b", nil},
		{"a\xffb\uFFFD", "a" + ch(1) + "b\uFFFD", nil},
		{"a☺\xffb☺\xC0\xAFc☺\xff", "a☺" + ch(1) + "b☺" + ch(2) + "c☺" + ch(1), nil},
		{"\xC0\xAF", ch(2), nil},
		{"\xE0\x80\xAF", ch(3), nil},
		{"\xed\xa0\x80", ch(3), nil},
		{"\xed\xbf\xbf", ch(3), nil},
		{"\xF0\x80\x80\xaf", ch(4), nil},
		{"\xF8\x80\x80\x80\xAF", ch(5), nil},
		{"\xFC\x80\x80\x80\x80\xAF", ch(6), nil},

		// {"\ud800", ""},
		{`\ud800`, ch(1), nil},
		{`\uDEAD`, ch(1), nil},

		{`\uD83D\ub000`, string([]byte{239, 191, 189, 235, 128, 128}), nil},
		{`\uD83D\ude04`, "😄", nil},

		{`\u4e2d\u6587`, "中文", nil},
		{`\ud83d\udc4a`, "\xf0\x9f\x91\x8a", nil},

		{`\U0001f64f`, ch(1), errors.New(`readEscapedChar: invalid escape char after`)},
		{`\uD83D\u00`, ch(1), errors.New(`readU4: expects 0~9 or a~f, but found`)},
	}

	eventPayload := []byte(`{"batch": [{"anonymousId":"anon_id","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`)
	for _, tt := range toValidUTF8Tests {
		sanitizedPayload, err := SanitizeJSON(bytes.Replace(eventPayload, []byte("track"), []byte(tt.in), 1))
		if tt.err != nil {
			require.Error(t, err, "should error")
			require.Contains(t, err.Error(), tt.err.Error(), "should contain error")
			continue
		}

		require.NoError(t, err)
		require.JSONEq(t,
			string(bytes.Replace(eventPayload, []byte("track"), []byte(tt.out), 1)),
			string(sanitizedPayload),
		)
	}
}

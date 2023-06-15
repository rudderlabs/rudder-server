package bot

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDetectBot(t *testing.T) {
	testCases := []struct {
		name      string
		userAgent string
		expected  bool
	}{
		{
			name:      "Googlebot",
			userAgent: "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
			expected:  true,
		},
		{
			name:      "Bingbot",
			userAgent: "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
			expected:  true,
		},
		{
			name:      "Not a bot",
			userAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
			expected:  false,
		},
		{
			name:      "Empty user agent",
			userAgent: "",
			expected:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, IsBotUserAgent(tc.userAgent))
		})
	}
}

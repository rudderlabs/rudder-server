package bot

import "strings"

var botKeyWords = []string{
	"bot",
	"crawler",
	"spider",
}

func IsBotUserAgent(userAgent string) bool {
	lowerUserAgent := strings.ToLower(userAgent)
	for _, keyword := range botKeyWords {
		if strings.Contains(lowerUserAgent, keyword) {
			return true
		}
	}
	return false
}

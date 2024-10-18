package snakecase

import (
	"strings"

	"github.com/samber/lo"
)

// ToSnakeCase converts a string to snake_case using regular word separation.
func ToSnakeCase(s string) string {
	return snakeCase(s, extractWords)
}

// ToSnakeCaseWithNumbers converts a string to snake_case, preserving numbers.
func ToSnakeCaseWithNumbers(s string) string {
	return snakeCase(s, extractWordsWithNumbers)
}

// snakeCase converts a string to snake_case based on a word extraction function.
func snakeCase(s string, wordExtractor func(s string) []string) string {
	s = strings.NewReplacer("'", "", "\u2019", "").Replace(s)
	words := wordExtractor(s)
	words = lo.Map(words, func(word string, _ int) string {
		return strings.ToLower(word)
	})
	return strings.Join(words, "_")
}

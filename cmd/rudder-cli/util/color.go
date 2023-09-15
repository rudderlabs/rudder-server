package util

import "fmt"

func RedColor(s string) string {
	return fmt.Sprintf("\033[31m%s\033[0m", s)
}

func GreenColor(s string) string {
	return fmt.Sprintf("\033[32m%s\033[0m", s)
}

package ratelimiter_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestRateLimiter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "RateLimiter Suite")
}

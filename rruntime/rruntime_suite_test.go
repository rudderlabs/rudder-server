package rruntime_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestRruntime(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Rruntime Suite")
}

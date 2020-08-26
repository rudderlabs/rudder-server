package batchrouter_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestBatchrouter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Batchrouter Suite")
}

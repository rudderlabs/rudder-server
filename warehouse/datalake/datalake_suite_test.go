package datalake_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDatalake(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Datalake Suite")
}

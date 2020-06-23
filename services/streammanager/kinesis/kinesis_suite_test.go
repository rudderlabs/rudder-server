package kinesis_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestKinesis(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Kinesis Suite")
}

package kinesis_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestKinesis(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Kinesis Suite")
}

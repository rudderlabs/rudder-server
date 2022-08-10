package azuresynapse_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAzureSynapse(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "AzureSynapse Suite")
}

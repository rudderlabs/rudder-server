//go:build !warehouse_integration

package configuration_testing

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestConfigurationTesting(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ConfigurationTesting Suite")
}

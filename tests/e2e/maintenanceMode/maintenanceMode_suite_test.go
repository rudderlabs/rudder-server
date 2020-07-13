package maintenanceMode_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestMaintenanceMode(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "MaintenanceMode Suite")
}

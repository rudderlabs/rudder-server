package migrations_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestMigrations(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Migrations Suite")
}

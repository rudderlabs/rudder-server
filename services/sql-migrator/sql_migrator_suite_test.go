package migrator_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestSqlMigrator(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SqlMigrator Suite")
}

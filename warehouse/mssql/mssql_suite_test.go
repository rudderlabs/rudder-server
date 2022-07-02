package mssql_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMssql(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Mssql Suite")
}

// +build enterprisemod

// This file contains enterprise exclusive dependencies.
// 	So that the go.mod file can be consistant regardless of enterprise code being present.
// To detect enterprise exclusive dependencies:
// 1. run `make enterprise-cleanup`
// 2. `go mod tidy`
// 3. observe the changes on go.mod `git diff go.mod`
package enterprisemod

import (
	_ "github.com/spaolacci/murmur3"
)

// +build dev

package migrator

import "net/http"

var MigrationAssets http.FileSystem = http.Dir("./sql/migrations")

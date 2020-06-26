// +build ignore

package main

import (
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/shurcooL/vfsgen"
)

func main() {
	err := vfsgen.Generate(migrator.MigrationAssets, vfsgen.Options{
		Filename:        "./services/sql-migrator/migrations_vfsdata.go",
		PackageName:     "migrator",
		BuildTags:       "!dev",
		VariableName:    "MigrationAssets",
		VariableComment: "MigrationAssets contains SQL migration scripts and templates",
	})
	if err != nil {
		panic(err)
	}
}

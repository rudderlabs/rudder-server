package migrations

import "embed"

//go:embed **/*.tmpl
//go:embed **/*.sql
var FS embed.FS

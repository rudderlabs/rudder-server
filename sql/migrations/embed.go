package migrations

import "embed"

//go:embed jobsdb/*.tmpl
//go:embed node/*.sql
//go:embed pg_notifier_queue/*.sql
//go:embed reports/*.sql
//go:embed warehouse/*.sql
var FS embed.FS

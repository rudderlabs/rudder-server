package rsources

import "database/sql"

type multitenantExtension struct {
	*defaultExtension
	sharedDB *sql.DB
}

func (r *multitenantExtension) getReadDB() *sql.DB {
	return r.sharedDB
}

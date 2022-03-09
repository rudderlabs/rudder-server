package jobsdb

import (
	"time"
)

type DBs struct {
	gwDBRetention     time.Duration
	routerDBRetention time.Duration
	migrationMode     string
	ClearDB           bool
	GatewayDB         HandleT
	RouterDB          HandleT
	BatchRouterDB     HandleT
	ProcErrDB         HandleT
	TenantRouterDB    MultiTenantHandleT
}

func (d *DBs) InitiateDBs(gwDBRetention, routerDBRetention time.Duration, migrationMode string, clearDB bool) {
	//dbs := factory()
	d.gwDBRetention = gwDBRetention
	d.routerDBRetention = routerDBRetention
	d.migrationMode = migrationMode
	d.ClearDB = clearDB

	d.GatewayDB.Setup(ReadWrite, d.ClearDB, "gw", d.gwDBRetention, d.migrationMode, true,
		QueryFiltersT{})
	d.RouterDB.Setup(ReadWrite, d.ClearDB, "rt", d.routerDBRetention, d.migrationMode, true,
		QueryFiltersT{})
	d.BatchRouterDB.Setup(ReadWrite, d.ClearDB, "batch_rt", d.routerDBRetention, d.migrationMode,
		true, QueryFiltersT{})
	d.ProcErrDB.Setup(ReadWrite, d.ClearDB, "proc_error", d.routerDBRetention, d.migrationMode, false,
		QueryFiltersT{})
}

func Factory() *DBs {
	db := DBs{
		GatewayDB:      HandleT{},
		RouterDB:       HandleT{},
		BatchRouterDB:  HandleT{},
		ProcErrDB:      HandleT{},
		TenantRouterDB: MultiTenantHandleT{},
	}
	db.TenantRouterDB.HandleT = &db.RouterDB
	return &db
}

func (d *DBs)Halt() {
	d.GatewayDB.HaltDB()
	d.RouterDB.HaltDB()
	d.BatchRouterDB.HaltDB()
	d.ProcErrDB.HaltDB()
}

func (d *DBs)Start() {
	d.GatewayDB.StartDB()
	d.RouterDB.StartDB()
	d.BatchRouterDB.StartDB()
	d.ProcErrDB.StartDB()
}

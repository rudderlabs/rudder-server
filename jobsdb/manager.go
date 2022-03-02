package jobsdb

import (
	"golang.org/x/sync/errgroup"
	"time"
)

type DBs struct {
	gwDBRetention     time.Duration
	routerDBRetention time.Duration
	migrationMode     string
	ClearDB           bool
	GatewayDB         HandleT
	gwDbWG            *errgroup.Group
	RouterDB          HandleT
	rtDbWG            *errgroup.Group
	BatchRouterDB     HandleT
	brtDbWG           *errgroup.Group
	ProcErrDB         HandleT
	prDbWG            *errgroup.Group
	TenantRouterDB    MultiTenantHandleT
}

func (d *DBs) InitiateDBs(gwDBRetention, routerDBRetention time.Duration, migrationMode string, clearDB bool) {

}

func (d *DBs) Start() {

}

func (d *DBs) Halt() {

}

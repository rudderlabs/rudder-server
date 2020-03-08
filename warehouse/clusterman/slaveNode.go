package clusterman

import "database/sql"

//SlaveNodeT will be used on warehouse workers
type SlaveNodeT struct {
	cn *clusterNodeT
}

//Setup to initialise
func (sn *SlaveNodeT) Setup(dbHandle *sql.DB, config *ClusterConfig) {
	sn.cn = &clusterNodeT{}
	sn.cn.Setup(sn, dbHandle, config)
}

//TearDown to release resources
func (sn *SlaveNodeT) TearDown() {
	sn.cn.TearDown()
}

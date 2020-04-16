package pathfinder

import (
	"sort"

	"github.com/rudderlabs/rudder-server/utils/logger"
)

//Pathfinder struct holds the current cluster state based on which it maps users to corresponding nodes
type Pathfinder struct {
	clusterState []NodeMeta
	// version			int
}

//NodeMeta struct holds the nodeId and its connection Info
type NodeMeta struct {
	nodeID           int
	connectionString string
}

//GetNodeID is a getter for nodeID
func (nMeta *NodeMeta) GetNodeID() int {
	return nMeta.nodeID
}

//GetNodeMeta returns a NodeMeta struct
func GetNodeMeta(nodeID int, connectionString string) NodeMeta {
	return NodeMeta{nodeID: nodeID, connectionString: connectionString}
}

//Setup sets the cluster state based on which users are routed to corresponding nodes
func (pf *Pathfinder) Setup(clusterState []NodeMeta) {
	//TODO: Look for duplicate nodeIds
	logger.Info("Shanmukh: inside pathfinder setup")

	sort.Slice(clusterState, func(i, j int) bool {
		return clusterState[i].nodeID < clusterState[j].nodeID
	})
	pf.clusterState = clusterState
}

//GetNodeFromHash returns NodeMeta from hash
func (pf *Pathfinder) GetNodeFromHash(hash uint32) NodeMeta {
	return pf.clusterState[int(hash)%len(pf.clusterState)]
}

//DoesNodeBelongToTheCluster returns a true if the passed nodeID is a part of the cluster
func (pf *Pathfinder) DoesNodeBelongToTheCluster(nodeID int) bool {
	for _, nMeta := range pf.clusterState {
		if nodeID == nMeta.GetNodeID() {
			return true
		}
	}
	return false
}

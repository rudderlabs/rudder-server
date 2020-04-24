package pathfinder

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/spaolacci/murmur3"
)

//Pathfinder struct holds the current cluster state based on which it maps users to corresponding nodes
type Pathfinder struct {
	clusterState []NodeMeta
	version      int
}

//NodeMeta struct holds the nodeId and its connection Info
type NodeMeta struct {
	nodeID           string
	connectionString string
}

//GetNodeID is a getter for nodeID
func (nMeta *NodeMeta) GetNodeID() string {
	return nMeta.nodeID
}

//GetNodeConnectionString is a getter for connectionString
func (nMeta *NodeMeta) GetNodeConnectionString(port int) string {
	return fmt.Sprintf("%s:%d", nMeta.connectionString, port)
}

//GetVersion returns of the current cluster
func (pf *Pathfinder) GetVersion() int {
	return pf.version
}

//GetNodeMeta is a constructor for NodeMeta struct
func GetNodeMeta(nodeID string, connectionString string) NodeMeta {
	return NodeMeta{nodeID: nodeID, connectionString: connectionString}
}

//Setup sets the cluster state based on which users are routed to corresponding nodes
func Setup(backendNodeCount int, version int, dnsPattern string, instanceIDPattern string) []NodeMeta {
	clusterInfo := []NodeMeta{}
	for i := 0; i < backendNodeCount; i++ {
		connectionString :=
			strings.ReplaceAll(
				strings.ReplaceAll(dnsPattern, "<CLUSTER_VERSION>", strconv.Itoa(version)),
				"<NODENUM>",
				strconv.Itoa(i))

		instanceID := strings.ReplaceAll(
			strings.ReplaceAll(instanceIDPattern, "<CLUSTER_VERSION>", strconv.Itoa(version)),
			"<NODENUM>",
			strconv.Itoa(i))
		nMeta := GetNodeMeta(instanceID, connectionString)
		clusterInfo = append(clusterInfo, nMeta)
	}
	return clusterInfo
}

//Setup sets the cluster state based on which users are routed to corresponding nodes
func (pf *Pathfinder) Setup(clusterState []NodeMeta, version int) {
	//TODO: Look for duplicate nodeIds

	sort.Slice(clusterState, func(i, j int) bool {
		return clusterState[i].nodeID < clusterState[j].nodeID
	})
	pf.clusterState = clusterState
	pf.version = version
	logger.Info("Pathfinder is setup %v", pf)
}

func (pf *Pathfinder) getNodeFromHash(hash uint32) NodeMeta {
	return pf.clusterState[int(hash)%len(pf.clusterState)]
}

//GetNodeFromID hashes the ID using murmur3 and uses getNodeFromHash to return the appropriate Node
func (pf *Pathfinder) GetNodeFromID(id string) NodeMeta {
	return pf.getNodeFromHash(murmur3.Sum32([]byte(id)))
}

//DoesNodeBelongToTheCluster returns a true if the passed nodeID is a part of the cluster
func (pf *Pathfinder) DoesNodeBelongToTheCluster(nodeID string) bool {
	for _, nMeta := range pf.clusterState {
		if nodeID == nMeta.GetNodeID() {
			return true
		}
	}
	return false
}

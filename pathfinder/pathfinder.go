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
func (nMeta *NodeMeta) GetNodeConnectionString() string {
	return nMeta.connectionString
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
func Setup(backendNodeCount int, version int, dnsPattern string, instanceIDPattern string, migratorPort int) []NodeMeta {
	clusterInfo := []NodeMeta{}
	for i := 0; i < backendNodeCount; i++ {
		dns :=
			strings.ReplaceAll(
				strings.ReplaceAll(dnsPattern, "<CLUSTER_VERSION>", strconv.Itoa(version)),
				"<NODENUM>",
				strconv.Itoa(i))

		connectionString := fmt.Sprintf("%s:%d", dns, migratorPort)

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

	mapForCheckingDuplicates := make(map[string]NodeMeta)
	for _, nMeta := range clusterState {
		if _, present := mapForCheckingDuplicates[nMeta.GetNodeID()]; present {
			panic("the node ids should be unique through out the cluster")
		} else {
			mapForCheckingDuplicates[nMeta.GetNodeID()] = nMeta
		}
	}

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

//GetNodeFromUserID hashes the ID using murmur3 and uses getNodeFromHash to return the appropriate Node
func (pf *Pathfinder) GetNodeFromUserID(userID string) NodeMeta {
	return pf.getNodeFromHash(murmur3.Sum32([]byte(userID)))
}

//GetNodeFromNodeID iterates over cluster state and returns the node meta corresponding to the nodeID given
func (pf *Pathfinder) GetNodeFromNodeID(nodeID string) NodeMeta {
	for _, nMeta := range pf.clusterState {
		if nMeta.GetNodeID() == nodeID {
			return nMeta
		}
	}
	return NodeMeta{}
}

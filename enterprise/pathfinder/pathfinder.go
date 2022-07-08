package pathfinder

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/spaolacci/murmur3"
)

var pkgLogger logger.LoggerI

func init() {
	pkgLogger = logger.NewLogger().Child("enterprise").Child("pathfinder")
}

// ClusterStateT struct holds the current cluster state based on which it maps users to corresponding nodes
type ClusterStateT struct {
	nodes []NodeT
}

// New Gives a new pathfinder
func New(nextClusterBackendCount, nextClusterVersion, migratorPort int, dnsPattern, instanceIDPattern string) ClusterStateT {
	var cs ClusterStateT
	cs = ClusterStateT{}

	// nodeList should reflect the nodes of the new cluster. If moving to inplace migrations, this should be appropriately changed
	nodeList := buildNodeList(nextClusterBackendCount, nextClusterVersion, dnsPattern, instanceIDPattern, migratorPort)

	cs.Setup(nodeList, nextClusterVersion)
	return cs
}

// NodeT struct holds the nodeId and its connection Info
type NodeT struct {
	ID               string
	connectionString string
}

// Setup sets the cluster state based on which users are routed to corresponding nodes
func buildNodeList(backendNodeCount, version int, dnsPattern, instanceIDPattern string, migratorPort int) []NodeT {
	nodeList := []NodeT{}
	for i := 0; i < backendNodeCount; i++ {
		dns := strings.ReplaceAll(
			strings.ReplaceAll(dnsPattern, "<CLUSTER_VERSION>", strconv.Itoa(version)),
			"<NODENUM>",
			strconv.Itoa(i))

		connectionString := fmt.Sprintf("%s:%d", dns, migratorPort)

		instanceID := strings.ReplaceAll(
			strings.ReplaceAll(instanceIDPattern, "<CLUSTER_VERSION>", strconv.Itoa(version)),
			"<NODENUM>",
			strconv.Itoa(i))
		node := NodeT{ID: instanceID, connectionString: connectionString}
		nodeList = append(nodeList, node)
	}
	return nodeList
}

// Setup sets the cluster state based on which users are routed to corresponding nodes
func (cs *ClusterStateT) Setup(nodes []NodeT, version int) {
	mapForCheckingDuplicates := make(map[string]NodeT)
	for _, node := range nodes {
		if _, present := mapForCheckingDuplicates[node.ID]; present {
			panic("the node ids should be unique through out the cluster")
		}

		mapForCheckingDuplicates[node.ID] = node
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
	cs.nodes = nodes

	pkgLogger.Info("Pathfinder is setup %v", cs)
}

// GetNodeFromUserID hashes the ID using murmur3 and uses getNodeFromHash to return the appropriate Node
func (cs *ClusterStateT) GetNodeFromUserID(userID string) NodeT {
	userIDHash := murmur3.Sum32([]byte(userID))
	return cs.nodes[int(userIDHash)%len(cs.nodes)]
}

// GetConnectionStringForNodeID iterates over cluster state and returns the node meta corresponding to the nodeID given
func (cs *ClusterStateT) GetConnectionStringForNodeID(nodeID string) (string, error) {
	for _, node := range cs.nodes {
		if node.ID == nodeID {
			return node.connectionString, nil
		}
	}
	return "", fmt.Errorf("node with ID : %s unknown to cluster %v", nodeID, cs)
}

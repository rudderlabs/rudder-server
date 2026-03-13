package clustertest

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/partmap"

	"github.com/rudderlabs/rudder-server/cluster/migrator/etcdclient"
)

type wpmh struct {
	workspaceID   string
	namespace     string
	client        etcdclient.Client
	numPartitions int
}

// NewWorkspacePartitionMappingHandler creates a handler which can read and write partition mappings for a workspace in etcd.
func NewWorkspacePartitionMappingHandler(client etcdclient.Client, numPartitions int, namespace, workspaceID string) *wpmh {
	return &wpmh{
		workspaceID:   workspaceID,
		namespace:     namespace,
		client:        client,
		numPartitions: numPartitions,
	}
}

// SetWorkspacePartitionMappings sets the partition mappings for the workspace in etcd.
func (wpmh *wpmh) SetWorkspacePartitionMappings(ctx context.Context, mappings partmap.PartitionIndexMapping) error {
	pm, err := mappings.ToPartitionMappingWithNumPartitions(uint32(wpmh.numPartitions))
	if err != nil {
		return fmt.Errorf("converting to partition mapping: %w", err)
	}
	v, err := pm.Marshal()
	if err != nil {
		return fmt.Errorf("marshaling partition mapping: %w", err)
	}
	_, err = wpmh.client.Put(ctx, workspacePartitionMappingKey(wpmh.workspaceID, wpmh.namespace), string(v))
	if err != nil {
		return fmt.Errorf("putting partition mapping: %w", err)
	}
	return nil
}

// GetWorkspacePartitionMappings gets the partition mappings for the workspace from etcd.
func (wpmh *wpmh) GetWorkspacePartitionMappings(ctx context.Context) (partmap.PartitionIndexMapping, error) {
	resp, err := wpmh.client.Get(ctx, workspacePartitionMappingKey(wpmh.workspaceID, wpmh.namespace))
	if err != nil {
		return nil, fmt.Errorf("getting partition mapping: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return partmap.PartitionIndexMapping{}, nil
	}
	var pm partmap.PartitionMapping
	err = pm.Unmarshal(resp.Kvs[0].Value)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling partition mapping: %w", err)
	}
	pim, err := pm.ToPartitionIndexMappingWithNumPartitions(uint32(wpmh.numPartitions))
	if err != nil {
		return nil, fmt.Errorf("converting to partition index mapping: %w", err)
	}
	return pim, nil
}

func workspacePartitionMappingKey(workspaceID, namespace string) string {
	return "/" + namespace + "/workspace_part_map/0/" + workspaceID
}

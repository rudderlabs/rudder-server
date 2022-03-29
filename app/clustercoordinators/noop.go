package clustercoordinator

import (
	"context"
)

type NOOPManager struct {
	Config *NOOPConfig
}

func GetNOOPConfig() *NOOPConfig {
	return &NOOPConfig{}
}

func (manager *NOOPManager) Get(ctx context.Context, key string) (string, error) {
	return key, nil
}

func (manager *NOOPManager) Put(ctx context.Context, key string, value string) error {
	return nil
}

func (manager *NOOPManager) Watch(ctx context.Context, key string) chan interface{} {
	resultChan := make(chan interface{}, 1)
	resultChan <- key
	return resultChan
}

func (manager *NOOPManager) WatchForWorkspaces(ctx context.Context, key string) chan string {
	resultChan := make(chan string, 1)
	resultChan <- key
	return resultChan
}

type NOOPConfig struct {
}

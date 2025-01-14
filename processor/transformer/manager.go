package transformer

import (
	"context"
	"errors"

	"github.com/rudderlabs/rudder-server/processor/types"
)

type ServiceClient interface {
	SendRequest(ctx context.Context, clientEvents []types.TransformerEvent, batchSize int) types.Response
}

type CommunicationManager struct {
	clients map[string]ServiceClient
}

func NewCommunicationManager() *CommunicationManager {
	return &CommunicationManager{
		clients: make(map[string]ServiceClient),
	}
}

func (m *CommunicationManager) RegisterService(name string, client ServiceClient) {
	m.clients[name] = client
}

func (m *CommunicationManager) GetServiceClient(name string) (ServiceClient, error) {
	client, exists := m.clients[name]
	if !exists {
		return nil, errors.New("service client not registered")
	}
	return client, nil
}

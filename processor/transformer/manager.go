package transformer

import "errors"

type ServiceClient interface {
	SendRequest(data interface{}) (response interface{}, err error)
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

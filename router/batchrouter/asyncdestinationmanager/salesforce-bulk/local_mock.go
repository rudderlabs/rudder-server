package salesforcebulk

import (
	"context"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/backend-config/identity"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

// MockSalesforceAPIService is a mock implementation of SalesforceAPIServiceInterface
type MockSalesforceAPIService struct {
	CreateJobFunc              func(objectName, operation, externalIDField string) (string, *APIError)
	UploadDataFunc             func(jobID, csvFilePath string) *APIError
	CloseJobFunc               func(jobID string) *APIError
	GetJobStatusFunc           func(jobID string) (*JobResponse, *APIError)
	GetFailedRecordsFunc       func(jobID string) ([]map[string]string, *APIError)
	GetSuccessfulRecordsFunc   func(jobID string) ([]map[string]string, *APIError)
	DeleteJobFunc              func(jobID string) *APIError
}

func (m *MockSalesforceAPIService) CreateJob(objectName, operation, externalIDField string) (string, *APIError) {
	if m.CreateJobFunc != nil {
		return m.CreateJobFunc(objectName, operation, externalIDField)
	}
	return "", nil
}

func (m *MockSalesforceAPIService) UploadData(jobID, csvFilePath string) *APIError {
	if m.UploadDataFunc != nil {
		return m.UploadDataFunc(jobID, csvFilePath)
	}
	return nil
}

func (m *MockSalesforceAPIService) CloseJob(jobID string) *APIError {
	if m.CloseJobFunc != nil {
		return m.CloseJobFunc(jobID)
	}
	return nil
}

func (m *MockSalesforceAPIService) GetJobStatus(jobID string) (*JobResponse, *APIError) {
	if m.GetJobStatusFunc != nil {
		return m.GetJobStatusFunc(jobID)
	}
	return &JobResponse{}, nil
}

func (m *MockSalesforceAPIService) GetFailedRecords(jobID string) ([]map[string]string, *APIError) {
	if m.GetFailedRecordsFunc != nil {
		return m.GetFailedRecordsFunc(jobID)
	}
	return []map[string]string{}, nil
}

func (m *MockSalesforceAPIService) GetSuccessfulRecords(jobID string) ([]map[string]string, *APIError) {
	if m.GetSuccessfulRecordsFunc != nil {
		return m.GetSuccessfulRecordsFunc(jobID)
	}
	return []map[string]string{}, nil
}

func (m *MockSalesforceAPIService) DeleteJob(jobID string) *APIError {
	if m.DeleteJobFunc != nil {
		return m.DeleteJobFunc(jobID)
	}
	return nil
}

// MockSalesforceAuthService is a mock implementation of SalesforceAuthServiceInterface
type MockSalesforceAuthService struct {
	AccessToken string
	InstanceURL string
	Error       error
}

func (m *MockSalesforceAuthService) GetAccessToken() (string, error) {
	if m.Error != nil {
		return "", m.Error
	}
	return m.AccessToken, nil
}

func (m *MockSalesforceAuthService) GetInstanceURL() string {
	return m.InstanceURL
}

// MockBackendConfig is a mock implementation of backendconfig.BackendConfig
type MockBackendConfig struct{}

func (m *MockBackendConfig) SetUp() error {
	return nil
}

func (m *MockBackendConfig) AccessToken() string {
	return ""
}

func (m *MockBackendConfig) Get(ctx context.Context) (map[string]backendconfig.ConfigT, error) {
	return nil, nil
}

func (m *MockBackendConfig) Identity() identity.Identifier {
	return &identity.EmptyIdentifier{}
}

func (m *MockBackendConfig) WaitForConfig(ctx context.Context) {}

func (m *MockBackendConfig) Subscribe(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
	return nil
}

func (m *MockBackendConfig) StartWithIDs(ctx context.Context, workspaces string) {}

func (m *MockBackendConfig) Stop() {}

// NewMockBackendConfig creates a new mock backend config
func NewMockBackendConfig() *MockBackendConfig {
	return &MockBackendConfig{}
}


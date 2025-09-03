package common

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/bing-ads-go-sdk/bingads"
	mockbulkservice "github.com/rudderlabs/bing-ads-go-sdk/mocks"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
)

// TestData contains common test data structures
type TestData struct {
	ValidDestinationConfig   map[string]interface{}
	ValidBaseConfig          *BaseDestinationConfig
	InvalidDestinationConfig map[string]interface{}
}

// GetTestData returns common test data for Bing Ads tests
func GetTestData() *TestData {
	return &TestData{
		ValidDestinationConfig: map[string]interface{}{
			"customerAccountId": "test-account",
			"customerId":        "test-customer",
			"rudderAccountID":   "test-rudder",
			"isHashRequired":    true,
		},
		ValidBaseConfig: &BaseDestinationConfig{
			CustomerAccountID: "test-account",
			CustomerID:        "test-customer",
			RudderAccountID:   "test-rudder",
			IsHashRequired:    true,
		},
		InvalidDestinationConfig: map[string]interface{}{
			"customerAccountId": "",
			"customerId":        "",
			"rudderAccountID":   "",
		},
	}
}

// CreateTestDestination creates a test destination with the given config
func CreateTestDestination(config map[string]interface{}, name string) *backendconfig.DestinationT {
	if name == "" {
		name = "test-destination"
	}

	return &backendconfig.DestinationT{
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name: name,
		},
		Config: config,
	}
}

// CreateTestBaseManager creates a test base manager with the given service
func CreateTestBaseManager(t *testing.T, service bingads.BulkServiceI, destConfig *BaseDestinationConfig) *BaseManager {
	t.Helper()

	if destConfig == nil {
		destConfig = GetTestData().ValidBaseConfig
	}

	return &BaseManager{
		Conf:            config.New(),
		Logger:          logger.NOP,
		StatsFactory:    stats.NOP,
		DestinationName: "test-destination",
		DestConfig:      destConfig,
		Service:         service,
	}
}

// SetupMockController creates and returns a gomock controller with cleanup
func SetupMockController(t *testing.T) (*gomock.Controller, func()) {
	ctrl := gomock.NewController(t)
	cleanup := func() {
		ctrl.Finish()
	}
	return ctrl, cleanup
}

// CreateMockBulkService creates a mock bulk service
func CreateMockBulkService(ctrl *gomock.Controller) bingads.BulkServiceI {
	return mockbulkservice.NewMockBulkServiceI(ctrl)
}

// TestCase represents a generic test case structure
type TestCase struct {
	Name        string
	Input       interface{}
	Expected    interface{}
	WantErr     bool
	ExpectedErr string
}

// CreateDestinationConfigTestCases creates test cases for destination config validation
func CreateDestinationConfigTestCases() []TestCase {
	return []TestCase{
		{
			Name: "valid config",
			Input: map[string]interface{}{
				"customerAccountId": "test-account",
				"customerId":        "test-customer",
				"rudderAccountID":   "test-rudder",
				"isHashRequired":    true,
			},
			WantErr: false,
		},
		{
			Name: "missing customerAccountId",
			Input: map[string]interface{}{
				"customerId":      "test-customer",
				"rudderAccountID": "test-rudder",
			},
			WantErr:     true,
			ExpectedErr: "customerAccountId is required",
		},
		{
			Name: "missing customerId",
			Input: map[string]interface{}{
				"customerAccountId": "test-account",
				"rudderAccountID":   "test-rudder",
			},
			WantErr:     true,
			ExpectedErr: "customerId is required",
		},
		{
			Name: "missing rudderAccountID",
			Input: map[string]interface{}{
				"customerAccountID": "test-account",
				"customerId":        "test-customer",
			},
			WantErr:     true,
			ExpectedErr: "rudderAccountID is required",
		},
		{
			Name: "empty strings",
			Input: map[string]interface{}{
				"customerAccountID": "",
				"customerId":        "",
				"rudderAccountID":   "",
			},
			WantErr:     true,
			ExpectedErr: "customerAccountId is required",
		},
		{
			Name: "whitespace strings",
			Input: map[string]interface{}{
				"customerAccountID": "   ",
				"customerId":        "test-customer",
				"rudderAccountID":   "test-rudder",
			},
			WantErr:     true,
			ExpectedErr: "customerAccountId is required",
		},
		{
			Name: "invalid type for customerAccountId",
			Input: map[string]interface{}{
				"customerAccountID": 123,
				"customerId":        "test-customer",
				"rudderAccountID":   "test-rudder",
			},
			WantErr:     true,
			ExpectedErr: "error in decoding destination config",
		},
		{
			Name:        "nil destination",
			Input:       (*backendconfig.DestinationT)(nil),
			WantErr:     true,
			ExpectedErr: "destination cannot be nil",
		},
	}
}

// CreateBaseConfigTestCases creates test cases for base config validation
func CreateBaseConfigTestCases() []TestCase {
	return []TestCase{
		{
			Name: "valid config",
			Input: &BaseDestinationConfig{
				CustomerAccountID: "test-account",
				CustomerID:        "test-customer",
				RudderAccountID:   "test-rudder",
				IsHashRequired:    true,
			},
			WantErr: false,
		},
		{
			Name: "valid config with false hash required",
			Input: &BaseDestinationConfig{
				CustomerAccountID: "test-account",
				CustomerID:        "test-customer",
				RudderAccountID:   "test-rudder",
				IsHashRequired:    false,
			},
			WantErr: false,
		},
		{
			Name: "missing customerAccountId",
			Input: &BaseDestinationConfig{
				CustomerID:      "test-customer",
				RudderAccountID: "test-rudder",
				IsHashRequired:  true,
			},
			WantErr:     true,
			ExpectedErr: "customerAccountId is required",
		},
		{
			Name: "missing customerId",
			Input: &BaseDestinationConfig{
				CustomerAccountID: "test-account",
				RudderAccountID:   "test-rudder",
				IsHashRequired:    true,
			},
			WantErr:     true,
			ExpectedErr: "customerId is required",
		},
		{
			Name: "missing rudderAccountID",
			Input: &BaseDestinationConfig{
				CustomerAccountID: "test-account",
				CustomerID:        "test-customer",
				IsHashRequired:    true,
			},
			WantErr:     true,
			ExpectedErr: "rudderAccountID is required",
		},
		{
			Name:        "all fields missing",
			Input:       &BaseDestinationConfig{},
			WantErr:     true,
			ExpectedErr: "customerAccountId is required",
		},
		{
			Name: "empty strings",
			Input: &BaseDestinationConfig{
				CustomerAccountID: "",
				CustomerID:        "",
				RudderAccountID:   "",
				IsHashRequired:    false,
			},
			WantErr:     true,
			ExpectedErr: "customerAccountId is required",
		},
		{
			Name: "whitespace strings",
			Input: &BaseDestinationConfig{
				CustomerAccountID: "   ",
				CustomerID:        "test-customer",
				RudderAccountID:   "test-rudder",
				IsHashRequired:    true,
			},
			WantErr:     true,
			ExpectedErr: "customerAccountId is required",
		},
		{
			Name:        "nil config",
			Input:       (*BaseDestinationConfig)(nil),
			WantErr:     true,
			ExpectedErr: "config cannot be nil",
		},
	}
}

func TestParseDestinationConfig(t *testing.T) {
	t.Parallel()

	testCases := CreateDestinationConfigTestCases()

	// Add additional specific test cases
	additionalCases := []TestCase{
		{
			Name: "valid config with all fields",
			Input: map[string]interface{}{
				"customerAccountId": "test-account-123",
				"customerId":        "test-customer-456",
				"rudderAccountId":   "test-rudder-789",
				"isHashRequired":    false,
			},
			WantErr: false,
		},
		{
			Name: "config with extra fields",
			Input: map[string]interface{}{
				"customerAccountId": "test-account",
				"customerId":        "test-customer",
				"rudderAccountId":   "test-rudder",
				"isHashRequired":    true,
				"extraField":        "extra-value",
				"anotherField":      123,
			},
			WantErr: false,
		},
		{
			Name: "config with different data types",
			Input: map[string]interface{}{
				"customerAccountId": 123,
				"customerId":        "test-customer",
				"rudderAccountId":   "test-rudder",
			},
			WantErr:     true,
			ExpectedErr: "error in decoding destination config",
		},
		{
			Name: "config with mixed valid and invalid types",
			Input: map[string]interface{}{
				"customerAccountId": "test-account",
				"customerId":        456, // Invalid type
				"rudderAccountId":   "test-rudder",
			},
			WantErr:     true,
			ExpectedErr: "error in decoding destination config",
		},
		{
			Name: "config with whitespace strings",
			Input: map[string]interface{}{
				"customerAccountId": "   ",
				"customerId":        "test-customer",
				"rudderAccountId":   "test-rudder",
			},
			WantErr:     true,
			ExpectedErr: "customerAccountId is required",
		},
		{
			Name: "nil config map",
			Input: &backendconfig.DestinationT{
				Config: nil,
			},
			WantErr:     true,
			ExpectedErr: "error in validating destination config",
		},
	}

	testCases = append(testCases, additionalCases...)

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			var destination *backendconfig.DestinationT
			if tc.Input == nil {
				destination = nil
			} else if configMap, ok := tc.Input.(map[string]interface{}); ok {
				destination = CreateTestDestination(configMap, "")
			} else {
				destination = tc.Input.(*backendconfig.DestinationT)
			}

			destConfig, err := ParseDestinationConfig(destination)

			if tc.WantErr {
				require.Error(t, err)
				if tc.ExpectedErr != "" {
					require.Contains(t, err.Error(), tc.ExpectedErr)
				}
				require.Nil(t, destConfig)
			} else {
				require.NoError(t, err)
				require.NotNil(t, destConfig)

				// Validate specific fields for valid configs
				if configMap, ok := tc.Input.(map[string]interface{}); ok {
					if customerAccountID, exists := configMap["customerAccountId"]; exists && customerAccountID != nil {
						require.Equal(t, customerAccountID, destConfig.CustomerAccountID)
					}
					if customerID, exists := configMap["customerId"]; exists && customerID != nil {
						require.Equal(t, customerID, destConfig.CustomerID)
					}
					if rudderAccountID, exists := configMap["rudderAccountId"]; exists && rudderAccountID != nil {
						require.Equal(t, rudderAccountID, destConfig.RudderAccountID)
					}
					if isHashRequired, exists := configMap["isHashRequired"]; exists && isHashRequired != nil {
						require.Equal(t, isHashRequired, destConfig.IsHashRequired)
					}
				}
			}
		})
	}
}

func TestValidateDestinationConfig(t *testing.T) {
	t.Parallel()

	testCases := CreateBaseConfigTestCases()

	// Add additional specific test cases
	additionalCases := []TestCase{
		{
			Name: "mixed valid and invalid fields - only customerAccountId valid",
			Input: &BaseDestinationConfig{
				CustomerAccountID: "test-account",
				CustomerID:        "",
				RudderAccountID:   "",
			},
			WantErr:     true,
			ExpectedErr: "customerId is required",
		},
		{
			Name: "mixed valid and invalid fields - only customerId valid",
			Input: &BaseDestinationConfig{
				CustomerAccountID: "",
				CustomerID:        "test-customer",
				RudderAccountID:   "",
			},
			WantErr:     true,
			ExpectedErr: "customerAccountId is required",
		},
		{
			Name: "mixed valid and invalid fields - only rudderAccountId valid",
			Input: &BaseDestinationConfig{
				CustomerAccountID: "",
				CustomerID:        "",
				RudderAccountID:   "test-rudder",
			},
			WantErr:     true,
			ExpectedErr: "customerAccountId is required",
		},
		{
			Name: "valid config with special characters",
			Input: &BaseDestinationConfig{
				CustomerAccountID: "test-account_123",
				CustomerID:        "test-customer@456",
				RudderAccountID:   "test-rudder#789",
				IsHashRequired:    true,
			},
			WantErr: false,
		},
		{
			Name: "config with only spaces",
			Input: &BaseDestinationConfig{
				CustomerAccountID: "   ",
				CustomerID:        "   ",
				RudderAccountID:   "   ",
				IsHashRequired:    false,
			},
			WantErr:     true,
			ExpectedErr: "customerAccountId is required",
		},
		{
			Name: "config with tabs and newlines",
			Input: &BaseDestinationConfig{
				CustomerAccountID: "\t\n",
				CustomerID:        "test-customer",
				RudderAccountID:   "test-rudder",
				IsHashRequired:    true,
			},
			WantErr:     true,
			ExpectedErr: "customerAccountId is required",
		},
	}

	testCases = append(testCases, additionalCases...)

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			config := tc.Input.(*BaseDestinationConfig)
			err := ValidateDestinationConfig(config)

			if tc.WantErr {
				require.Error(t, err)
				if tc.ExpectedErr != "" {
					require.Contains(t, err.Error(), tc.ExpectedErr)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNewBaseManager(t *testing.T) {
	t.Parallel()

	t.Run("successful creation", func(t *testing.T) {
		t.Parallel()

		ctrl, cleanup := SetupMockController(t)
		defer cleanup()

		destination := CreateTestDestination(GetTestData().ValidDestinationConfig, "test-destination")
		mockService := CreateMockBulkService(ctrl)

		destConfig, err := ParseDestinationConfig(destination)
		require.NoError(t, err)

		baseManager := CreateTestBaseManager(t, mockService, destConfig)

		require.NotNil(t, baseManager)
		require.Equal(t, "test-rudder", baseManager.DestConfig.RudderAccountID)
		require.Equal(t, "test-destination", baseManager.DestinationName)
		require.Equal(t, mockService, baseManager.Service)
	})

	t.Run("config parsing with extra fields", func(t *testing.T) {
		t.Parallel()

		destination := CreateTestDestination(map[string]interface{}{
			"customerAccountId": "test-account",
			"customerId":        "test-customer",
			"rudderAccountId":   "test-rudder",
			"extraField":        "extra-value",
			"anotherField":      123,
		}, "")

		destConfig, err := ParseDestinationConfig(destination)
		require.NoError(t, err)
		require.Equal(t, "test-account", destConfig.CustomerAccountID)
		require.Equal(t, "test-customer", destConfig.CustomerID)
		require.Equal(t, "test-rudder", destConfig.RudderAccountID)
	})

	t.Run("config parsing with boolean fields", func(t *testing.T) {
		t.Parallel()

		destination := CreateTestDestination(map[string]interface{}{
			"customerAccountId": "test-account",
			"customerId":        "test-customer",
			"rudderAccountId":   "test-rudder",
			"isHashRequired":    true,
			"isEnabled":         false,
		}, "")

		destConfig, err := ParseDestinationConfig(destination)
		require.NoError(t, err)
		require.Equal(t, "test-account", destConfig.CustomerAccountID)
		require.Equal(t, "test-customer", destConfig.CustomerID)
		require.Equal(t, "test-rudder", destConfig.RudderAccountID)
		require.True(t, destConfig.IsHashRequired)
	})

	t.Run("config parsing with empty strings", func(t *testing.T) {
		t.Parallel()

		destination := CreateTestDestination(GetTestData().InvalidDestinationConfig, "")

		_, err := ParseDestinationConfig(destination)
		require.Error(t, err)
		require.Contains(t, err.Error(), "customerAccountId is required")
	})

	t.Run("config parsing with nil values", func(t *testing.T) {
		t.Parallel()

		destination := CreateTestDestination(map[string]interface{}{
			"customerAccountId": nil,
			"customerId":        "test-customer",
			"rudderAccountID":   "test-rudder",
		}, "")

		_, err := ParseDestinationConfig(destination)
		require.Error(t, err)
		require.Contains(t, err.Error(), "customerAccountId is required")
	})

	t.Run("config parsing with mixed types", func(t *testing.T) {
		t.Parallel()

		destination := CreateTestDestination(map[string]interface{}{
			"customerAccountId": "test-account",
			"customerId":        123, // Invalid type
			"rudderAccountId":   "test-rudder",
		}, "")

		_, err := ParseDestinationConfig(destination)
		require.Error(t, err)
		require.Contains(t, err.Error(), "error in decoding destination config")
	})
}

func TestNewBaseManager_Integration(t *testing.T) {
	t.Parallel()

	t.Run("invalid destination config", func(t *testing.T) {
		t.Parallel()

		destination := CreateTestDestination(map[string]interface{}{}, "test-destination")

		conf := config.New()
		ctrl, cleanup := SetupMockController(t)
		defer cleanup()

		mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(ctrl)

		baseManager, err := NewBaseManager(conf, logger.NOP, stats.NOP, mockBackendConfig, destination)
		require.Error(t, err)
		require.Nil(t, baseManager)
		require.Contains(t, err.Error(), "error in validating destination config")
	})
}

func TestBaseManager_Fields(t *testing.T) {
	t.Parallel()

	t.Run("base manager field access", func(t *testing.T) {
		t.Parallel()

		baseManager := CreateTestBaseManager(t, nil, GetTestData().ValidBaseConfig)

		require.NotNil(t, baseManager.Conf)
		require.NotNil(t, baseManager.Logger)
		require.NotNil(t, baseManager.StatsFactory)
		require.Equal(t, "test-destination", baseManager.DestinationName)
		require.NotNil(t, baseManager.DestConfig)
		require.Equal(t, "test-account", baseManager.DestConfig.CustomerAccountID)
		require.Equal(t, "test-customer", baseManager.DestConfig.CustomerID)
		require.Equal(t, "test-rudder", baseManager.DestConfig.RudderAccountID)
		require.True(t, baseManager.DestConfig.IsHashRequired)
	})

	t.Run("base manager with different config values", func(t *testing.T) {
		t.Parallel()

		baseManager := CreateTestBaseManager(t, nil, &BaseDestinationConfig{
			CustomerAccountID: "another-account",
			CustomerID:        "another-customer",
			RudderAccountID:   "another-rudder",
			IsHashRequired:    false,
		})

		require.NotNil(t, baseManager.Conf)
		require.NotNil(t, baseManager.Logger)
		require.NotNil(t, baseManager.StatsFactory)
		require.Equal(t, "test-destination", baseManager.DestinationName)
		require.NotNil(t, baseManager.DestConfig)
		require.Equal(t, "another-account", baseManager.DestConfig.CustomerAccountID)
		require.Equal(t, "another-customer", baseManager.DestConfig.CustomerID)
		require.Equal(t, "another-rudder", baseManager.DestConfig.RudderAccountID)
		require.False(t, baseManager.DestConfig.IsHashRequired)
	})
}

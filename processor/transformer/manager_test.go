package transformer

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	mock_transformer "github.com/rudderlabs/rudder-server/mocks/processor"
)

func TestCommunicationManager(t *testing.T) {
	manager := NewCommunicationManager()
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	t.Run("Successful Register", func(t *testing.T) {
		manager.RegisterService("test", mock_transformer.NewMockServiceClient(mockCtrl))
		client, err := manager.GetServiceClient("test")
		require.NoError(t, err)
		require.NotNil(t, client)
	})

	t.Run("Invalid Register", func(t *testing.T) {
		expectedErr := errors.New("service client not registered")
		client, err := manager.GetServiceClient("test-2")
		require.ErrorAs(t, err, &expectedErr)
		require.Nil(t, client)
	})
}

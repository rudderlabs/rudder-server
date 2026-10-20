package fileuploader

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_backendconfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

func TestFileUploaderUpdatingWithConfigBackend(t *testing.T) {
	ctrl := gomock.NewController(t)
	config := mock_backendconfig.NewMockBackendConfig(ctrl)

	configCh := make(chan pubsub.DataEvent)

	config.EXPECT().Subscribe(
		gomock.Any(),
		gomock.Eq(backendconfig.TopicBackendConfig),
	).DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
		go func() {
			<-ctx.Done()
			close(configCh)
		}()

		return configCh
	})
	ctx := t.Context()
	// Given I have a fileUploaderProvider reading from the backend
	fileUploaderProvider := NewProvider(ctx, config)
	var err error
	var preferences backendconfig.StoragePreferences

	// When the config backend has not published any event yet
	ctx2, cancel2 := context.WithTimeout(ctx, 50*time.Millisecond)
	preferences, err = fileUploaderProvider.GetStoragePreferences(ctx2, "testWorkspaceId-1")
	require.EqualValues(t, backendconfig.StoragePreferences{}, preferences)
	require.Equal(t, context.DeadlineExceeded, err)
	cancel2()

	t.Setenv("JOBS_BACKUP_STORAGE_PROVIDER", "S3") // default rudder storage provider
	t.Setenv("JOBS_BACKUP_DEFAULT_PREFIX", "defaultPrefixWithStorageTTL")
	t.Setenv("JOBS_BACKUP_PREFIX", "fullStoragePrefixWithNoTTL")

	// When user has not configured any storage
	configCh <- pubsub.DataEvent{
		Data: map[string]backendconfig.ConfigT{
			"testWorkspaceId-1": {
				WorkspaceID: "testWorkspaceId-1",
				Settings: backendconfig.Settings{
					DataRetention: backendconfig.DataRetention{
						UseSelfStorage: false,
						StorageBucket: backendconfig.StorageBucket{
							Type: "S3",
						},
						StoragePreferences: backendconfig.StoragePreferences{
							GatewayDumps: false,
						},
						RetentionPeriod: "full",
					},
				},
			},
			"testWorkspaceId-2": {
				WorkspaceID: "testWorkspaceId-2",
				Settings: backendconfig.Settings{
					DataRetention: backendconfig.DataRetention{
						UseSelfStorage: true,
						StorageBucket: backendconfig.StorageBucket{
							Type:   "",
							Config: map[string]any{},
						},
						StoragePreferences: backendconfig.StoragePreferences{
							GatewayDumps: false,
						},
					},
				},
			},
			"testWorkspaceId-3": {
				WorkspaceID: "testWorkspaceId-3",
				Settings: backendconfig.Settings{
					DataRetention: backendconfig.DataRetention{
						UseSelfStorage: false,
						StorageBucket: backendconfig.StorageBucket{
							Type:   "S3",
							Config: map[string]any{},
						},
						StoragePreferences: backendconfig.StoragePreferences{
							GatewayDumps: false,
						},
						RetentionPeriod: "default",
					},
				},
			},
		},
		Topic: string(backendconfig.TopicBackendConfig),
	}

	require.Eventually(t, func() bool {
		fm1, err := fileUploaderProvider.GetFileManager(ctx, "testWorkspaceId-1")
		if err != nil {
			return false
		}
		if fm1 == nil || fm1.Prefix() != "fullStoragePrefixWithNoTTL" {
			return false
		}

		fm3, err := fileUploaderProvider.GetFileManager(ctx, "testWorkspaceId-3")
		if err != nil {
			return false
		}
		if fm3 == nil || fm3.Prefix() != "defaultPrefixWithStorageTTL" {
			return false
		}

		fm0, err := fileUploaderProvider.GetFileManager(ctx, "testWorkspaceId-0")
		if err != ErrNoStorageForWorkspace {
			return false
		}
		if fm0 != nil {
			return false
		}

		fm2, err := fileUploaderProvider.GetFileManager(ctx, "testWorkspaceId-2")
		if err != ErrNoStorageForWorkspace {
			return false
		}
		if fm2 != nil {
			return false
		}

		preferences, err := fileUploaderProvider.GetStoragePreferences(ctx, "testWorkspaceId-1")
		if err != nil {
			return false
		}
		if preferences != (backendconfig.StoragePreferences{
			GatewayDumps: false,
		}) {
			return false
		}

		preferences, err = fileUploaderProvider.GetStoragePreferences(ctx, "testWorkspaceId-0")
		if err != ErrNoStorageForWorkspace {
			return false
		}
		if preferences != (backendconfig.StoragePreferences{}) {
			return false
		}

		preferences, err = fileUploaderProvider.GetStoragePreferences(ctx, "testWorkspaceId-2")
		if err != ErrNoStorageForWorkspace {
			return false
		}
		if preferences != (backendconfig.StoragePreferences{}) {
			return false
		}

		return true
	}, time.Second, 50*time.Millisecond)

	configCh <- pubsub.DataEvent{
		Data: map[string]backendconfig.ConfigT{
			"testWorkspaceId-2": {
				WorkspaceID: "testWorkspaceId-2",
				Settings: backendconfig.Settings{
					DataRetention: backendconfig.DataRetention{
						UseSelfStorage: true,
						StorageBucket: backendconfig.StorageBucket{
							Type:   "S3",
							Config: map[string]any{},
						},
						StoragePreferences: backendconfig.StoragePreferences{
							GatewayDumps: false,
						},
					},
				},
			},
		},
	}
	require.Eventually(t, func() bool {
		t.Log("testWorkspaceId-2 uploader not updated yet")
		_, err := fileUploaderProvider.GetFileManager(ctx, "testWorkspaceId-2")
		return err == nil
	}, time.Second, 5*time.Millisecond)
}

func TestFileUploaderWithoutConfigUpdates(t *testing.T) {
	ctrl := gomock.NewController(t)
	config := mock_backendconfig.NewMockBackendConfig(ctrl)

	configCh := make(chan pubsub.DataEvent)

	var ready sync.WaitGroup
	ready.Add(1)

	config.EXPECT().Subscribe(
		gomock.Any(),
		gomock.Eq(backendconfig.TopicBackendConfig),
	).DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
		ready.Done()
		close(configCh)
		return configCh
	})

	p := NewProvider(t.Context(), config)
	ready.Wait()
	_, err := p.GetFileManager(t.Context(), "testWorkspaceId-1")
	require.Equal(t, ErrNotSubscribed, err)
}

func TestStaticProvider(t *testing.T) {
	prefs := backendconfig.StoragePreferences{
		GatewayDumps: true,
	}

	storageSettings := map[string]StorageSettings{
		"testWorkspaceId-1": {
			Bucket: backendconfig.StorageBucket{
				Type:   "S3",
				Config: map[string]any{},
			},
			Preferences: prefs,
		},
	}
	p := NewStaticProvider(storageSettings)

	gotPrefs, err := p.GetStoragePreferences(t.Context(), "testWorkspaceId-1")
	require.NoError(t, err)
	require.EqualValues(t, prefs, gotPrefs)

	_, err = p.GetFileManager(t.Context(), "testWorkspaceId-1")
	require.NoError(t, err)
}

func TestDefaultProvider(t *testing.T) {
	d := NewDefaultProvider()

	prefs, err := d.GetStoragePreferences(t.Context(), "")
	require.NoError(t, err)
	require.EqualValues(t, backendconfig.StoragePreferences{
		GatewayDumps: true,
	}, prefs)

	_, err = d.GetFileManager(t.Context(), "")
	require.NoError(t, err)
}

func TestOverride(t *testing.T) {
	config := map[string]any{
		"a": "1",
		"b": "2",
		"c": "3",
	}
	settings := backendconfig.StorageBucket{
		Type: "S3",
		Config: map[string]any{
			"b": "4",
			"d": "5",
		},
	}
	bucket := overrideWithSettings(config, settings, "wrk-1")
	require.Equal(t, map[string]any{
		"a": "1",
		"b": "4",
		"c": "3",
		"d": "5",
	}, bucket.Config)

	config = map[string]any{
		"a":          "1",
		"b":          "2",
		"c":          "3",
		"iamRoleArn": "abc",
	}
	settings = backendconfig.StorageBucket{
		Type: "S3",
		Config: map[string]any{
			"b": "4",
			"d": "5",
		},
	}

	bucket = overrideWithSettings(config, settings, "wrk-1")
	require.Equal(t, map[string]any{
		"a":          "1",
		"b":          "4",
		"c":          "3",
		"d":          "5",
		"iamRoleArn": "abc",
		"externalID": "wrk-1",
	}, bucket.Config)

	config = map[string]any{
		"a":      "1",
		"b":      "2",
		"c":      "3",
		"region": "us-east-1",
	}
	settings = backendconfig.StorageBucket{
		Type: "S3",
	}
	bucket = overrideWithSettings(config, settings, "wrk-1")
	require.Equal(t, map[string]any{
		"a": "1",
		"b": "2",
		"c": "3",
	}, bucket.Config)

	config = map[string]any{
		"a":      "1",
		"b":      "2",
		"c":      "3",
		"region": "us-east-1",
	}
	settings = backendconfig.StorageBucket{
		Type: "MINIO",
	}
	bucket = overrideWithSettings(config, settings, "wrk-1")
	require.Equal(t, map[string]any{
		"a":      "1",
		"b":      "2",
		"c":      "3",
		"region": "us-east-1",
	}, bucket.Config)
}

package api

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/filemanager/mock_filemanager"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

func requireFileClosed(t *testing.T, f *os.File) {
	t.Helper()
	require.NotNil(t, f)
	_, err := f.Write([]byte("x"))
	require.Error(t, err)
	require.Error(t, f.Close(), "expected file to already be closed")
}

func TestValidateObjectStorageClosesFiles(t *testing.T) {
	validations.Init()
	misc.Init()

	tmpDir := t.TempDir()
	t.Setenv("RUDDER_TMPDIR", tmpDir)
	config.Reset()
	config.Set("RUDDER_TMPDIR", tmpDir)
	t.Cleanup(config.Reset)

	request := validateObjectStorageRequest{
		Type:   "S3",
		Config: map[string]any{"bucketName": "test-bucket"},
	}

	t.Run("success closes upload and download files", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		mockFileManager := mock_filemanager.NewMockFileManager(mockCtrl)
		mockFileManager.EXPECT().Prefix().Return("")
		mockFileManager.EXPECT().ListFilesWithPrefix(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(filemanager.MockListSession(nil, nil))

		var uploadFile, downloadFile *os.File
		mockFileManager.EXPECT().Upload(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, f *os.File, _ ...string) (filemanager.UploadedFile, error) {
				uploadFile = f
				return filemanager.UploadedFile{Location: "s3://bucket/key", ObjectName: "key"}, nil
			},
		)
		mockFileManager.EXPECT().GetDownloadKeyFromFileLocation(gomock.Any()).Return("key")
		mockFileManager.EXPECT().Download(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, w io.WriterAt, _ string, _ ...filemanager.DownloadOption) error {
				f, ok := w.(*os.File)
				require.True(t, ok)
				downloadFile = f
				return nil
			},
		)

		g := &GRPC{
			conf: config.New(),
			fileManagerFactory: func(_ *filemanager.Settings) (filemanager.FileManager, error) {
				return mockFileManager, nil
			},
		}

		require.NoError(t, g.validateObjectStorage(context.Background(), request))
		requireFileClosed(t, uploadFile)
		requireFileClosed(t, downloadFile)
	})

	t.Run("upload error still closes upload file", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		mockFileManager := mock_filemanager.NewMockFileManager(mockCtrl)
		mockFileManager.EXPECT().Prefix().Return("")
		mockFileManager.EXPECT().ListFilesWithPrefix(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(filemanager.MockListSession(nil, nil))

		var uploadFile *os.File
		mockFileManager.EXPECT().Upload(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, f *os.File, _ ...string) (filemanager.UploadedFile, error) {
				uploadFile = f
				return filemanager.UploadedFile{}, errors.New("upload failed")
			},
		)

		g := &GRPC{
			conf: config.New(),
			fileManagerFactory: func(_ *filemanager.Settings) (filemanager.FileManager, error) {
				return mockFileManager, nil
			},
		}

		err := g.validateObjectStorage(context.Background(), request)
		require.Error(t, err)
		requireFileClosed(t, uploadFile)
	})
}

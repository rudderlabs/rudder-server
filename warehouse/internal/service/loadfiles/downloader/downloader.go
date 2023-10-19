package downloader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type Downloader interface {
	Download(ctx context.Context, tableName string) ([]string, error)
}

type downloaderImpl struct {
	warehouse  *model.Warehouse
	uploader   warehouseutils.Uploader
	numWorkers int
}

func NewDownloader(
	warehouse *model.Warehouse,
	uploader warehouseutils.Uploader,
	numWorkers int,
) Downloader {
	return &downloaderImpl{
		warehouse:  warehouse,
		uploader:   uploader,
		numWorkers: numWorkers,
	}
}

func (l *downloaderImpl) Download(ctx context.Context, tableName string) ([]string, error) {
	var (
		fileNames     []string
		objectName    string
		err           error
		fileNamesLock sync.RWMutex
	)

	objects, err := l.uploader.GetLoadFilesMetadata(ctx, warehouseutils.GetLoadFilesOptions{Table: tableName})
	if err != nil {
		return nil, fmt.Errorf("getting load files metadata: %w", err)
	}
	storageProvider := warehouseutils.ObjectStorageType(
		l.warehouse.Destination.DestinationDefinition.Name,
		l.warehouse.Destination.Config,
		l.uploader.UseRudderStorage(),
	)

	fileManager, err := filemanager.New(&filemanager.Settings{
		Provider: storageProvider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         storageProvider,
			Config:           l.warehouse.Destination.Config,
			UseRudderStorage: l.uploader.UseRudderStorage(),
			WorkspaceID:      l.warehouse.Destination.WorkspaceID,
		}),
	})
	if err != nil {
		return nil, fmt.Errorf("creating filemanager for destination: %w", err)
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(l.numWorkers)

	for _, object := range objects {
		object := object

		g.Go(func() error {
			if objectName, err = l.downloadSingleObject(ctx, fileManager, object); err != nil {
				return fmt.Errorf("downloading object: %w", err)
			}

			fileNamesLock.Lock()
			fileNames = append(fileNames, objectName)
			fileNamesLock.Unlock()
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return nil, fmt.Errorf("downloading batch: %w", err)
	}
	return fileNames, nil
}

func (l *downloaderImpl) downloadSingleObject(ctx context.Context, fileManager filemanager.FileManager, object warehouseutils.LoadFile) (string, error) {
	var (
		objectName string
		tmpDirPath string
		err        error
		objectFile *os.File
	)

	ObjectStorage := warehouseutils.ObjectStorageType(
		l.warehouse.Destination.DestinationDefinition.Name,
		l.warehouse.Destination.Config,
		l.uploader.UseRudderStorage(),
	)

	if objectName, err = warehouseutils.GetObjectName(object.Location, l.warehouse.Destination.Config, ObjectStorage); err != nil {
		return "", fmt.Errorf("object name for location: %s, %w", object.Location, err)
	}

	dirName := fmt.Sprintf(`/%s/`, misc.RudderWarehouseLoadUploadsTmp)
	if tmpDirPath, err = misc.CreateTMPDIR(); err != nil {
		return "", fmt.Errorf("creating tmp dir: %w", err)
	}

	ObjectPath := tmpDirPath + dirName + fmt.Sprintf(`%s_%s_%d/`, l.warehouse.Destination.DestinationDefinition.Name, l.warehouse.Destination.ID, time.Now().Unix()) + objectName
	if err = os.MkdirAll(filepath.Dir(ObjectPath), os.ModePerm); err != nil {
		return "", fmt.Errorf("making tmp dir: %w", err)
	}

	if objectFile, err = os.Create(ObjectPath); err != nil {
		return "", fmt.Errorf("creating file in tmp dir: %w", err)
	}

	if err = fileManager.Download(ctx, objectFile, objectName); err != nil {
		return "", fmt.Errorf("downloading file from object storage: %w", err)
	}

	if err = objectFile.Close(); err != nil {
		return "", fmt.Errorf("closing downloaded file in tmp directory: %w", err)
	}

	return objectFile.Name(), nil
}

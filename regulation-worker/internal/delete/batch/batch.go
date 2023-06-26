package batch

// This is going to call appropriate method of Filemanager & DeleteManager
// to get deletion done.
// called by delete/deleteSvc with (model.Job, model.Destination).
// returns final status,error ({successful, failure}, err)
import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	_ "go.uber.org/automaxprocs"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/batch/filehandler"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

var (
	pkgLogger             = logger.NewLogger().Child("batch")
	StatusTrackerFileName = "rudderDeleteTracker.txt"
	supportedDestinations = []string{"S3", "S3_DATALAKE"}
)

type Batch struct {
	mu         sync.Mutex
	FM         filemanager.FileManager
	session    filemanager.ListSession
	TmpDirPath string
}

// listFiles fetches the files from filemanager under prefix mentioned and for a
// specified limit.
func (b *Batch) listFiles(ctx context.Context, prefix string, limit int) (fileObjects []*filemanager.FileInfo, err error) {
	pkgLogger.Debugf("getting a list of files from destination under prefix: %s with limit: %d", prefix, limit)
	if b.session == nil {
		b.session = b.FM.ListFilesWithPrefix(ctx, "", prefix, int64(limit))
	}

	if fileObjects, err = b.session.Next(); err != nil {
		return []*filemanager.FileInfo{}, fmt.Errorf("list files under prefix: %s and limit: %d from filemanager: %v", prefix, limit, err)
	}

	return
}

// two pointer algorithm implementation to remove all the files from which users are already deleted.
func removeCleanedFiles(files []*filemanager.FileInfo, cleanedFiles []string) []*filemanager.FileInfo {
	pkgLogger.Debugf("removing already cleaned files")
	sort.Slice(files, func(i, j int) bool {
		return files[i].Key < files[j].Key
	})
	sort.Slice(cleanedFiles, func(i, j int) bool {
		return cleanedFiles[i] < cleanedFiles[j]
	})

	i := 0
	j := 0
	presentCount := 0
	present := make([]bool, len(files))
	for j < len(cleanedFiles) {
		if files[i].Key < cleanedFiles[j] {
			i++
		} else if files[i].Key > cleanedFiles[j] {
			j++
		} else {
			present[i] = true
			presentCount++
			i++
			j++
		}
	}
	j = 0
	finalFiles := make([]*filemanager.FileInfo, len(files)-presentCount)

	for i := 0; i < len(files); i++ {
		if !present[i] {
			finalFiles[j] = files[i]
			j++
		}
	}
	return finalFiles
}

// append <fileName> to <statusTrackerFile> locally for which deletion has completed.
// updateStatusTrackerFile updates the tracker file with the fileName information
func (*Batch) updateStatusTrackerFile(absStatusTrackerFileName, fileName string) error {
	f, err := os.OpenFile(absStatusTrackerFileName, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("error while opening file, %w", err)
	}

	defer func() {
		_ = f.Close()
	}()
	if _, err := fmt.Fprintf(f, "%s\n", fileName); err != nil {
		return fmt.Errorf("error while writing to statusTrackerFile: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("closing the file:%s, err: %w", absStatusTrackerFileName, err)
	}

	return nil
}

func (*Batch) cleanedFiles(_ context.Context, path string, job *model.Job) ([]string, error) {
	pkgLogger.Debugf("fetching already cleaned files based on contents of the status tracker file")

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open status tracker file: %s, err: %w", path, err)
	}

	defer f.Close()

	byt, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read contents of status tracker file: %s, err: %w", path, err)
	}
	// if statusTracker.txt exists then read it & remove all those files name from above gzFilesObjects,
	// since those files are already cleaned.

	jobID := fmt.Sprintf("%d", job.ID)

	if len(byt) == 0 {
		// insert <jobID> in 1st line
		if _, err := fmt.Fprintf(f, "%s\n", jobID); err != nil {
			return nil, fmt.Errorf("writing jobId: %s to status tracker file: %s, err: %w", jobID, StatusTrackerFileName, err)
		}
		return nil, nil
	}

	lines := strings.Split(string(byt), "\n")
	// check if our <jobID> matches with the one in file.
	// if not, then truncate the file & write new current jobID.

	// This might happen when we have a job partially working on the
	// suppress with delete and then it fails and second job starts in the meantime.
	// So we keep the latest state in here.
	if lines[0] != jobID {

		// truncate the contents of the file, to start writing for another
		// <jobID> information.
		if err := f.Truncate(0); err != nil {
			return nil, fmt.Errorf("truncate the original file: %s, err: %w", path, err)
		}

		if _, err := f.Seek(0, 0); err != nil {
			return nil, fmt.Errorf("moving seek pointer: %s to zero location: %w", path, err)
		}
		if _, err := fmt.Fprintf(f, "%s\n", jobID); err != nil {
			return nil, fmt.Errorf("writing to status tracker file:%s, err: %w", StatusTrackerFileName, err)
		}

		return nil, nil
	}

	// if we have entries then read it.
	if len(lines) >= 1 {
		return lines[1:], nil
	}

	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("closing file: %w", err)
	}

	return nil, nil
}

// downloads `fileName` locally. And returns empty file, if file not found.
// Note: download happens concurrently in 5 go routine by default.
func (b *Batch) download(ctx context.Context, completeFileName string) (string, error) {
	pkgLogger.Infof("downloading file: %s locally", completeFileName)

	tmpFilePathPrefix, err := os.MkdirTemp(b.TmpDirPath, "")
	if err != nil {
		return "", fmt.Errorf("create temporary directory: %w", err)
	}

	_, fileName := filepath.Split(completeFileName)
	tmpFilePtr, err := os.OpenFile(filepath.Join(tmpFilePathPrefix, fileName), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return "", fmt.Errorf("opening file: %s, %w", fileName, err)
	}

	defer func() {
		_ = tmpFilePtr.Close()
	}()

	absPath, err := filepath.Abs(tmpFilePtr.Name())
	if err != nil {
		return "", fmt.Errorf("getting absolute path for: %s, %w", tmpFilePtr.Name(), err)
	}

	err = b.FM.Download(ctx, tmpFilePtr, completeFileName)
	if err != nil {
		if err == filemanager.ErrKeyNotFound {
			pkgLogger.Debugf("file not found")
			return absPath, nil
		}
		return "", fmt.Errorf("downloading object: %s using file manager: %w", completeFileName, err)
	}

	if err := tmpFilePtr.Close(); err != nil {
		return "", fmt.Errorf("closing the tmp file: %s", err.Error())
	}

	return absPath, nil
}

func downloadWithExpBackoff(ctx context.Context, fu func(context.Context, string) (string, error), fileName string) (string, error) {
	pkgLogger.Debugf("downloading file: %s with exponential backoff", fileName)

	maxWait := time.Minute * 10
	bo := backoff.NewExponentialBackOff()
	boCtx := backoff.WithContext(bo, ctx)
	bo.MaxInterval = time.Minute
	bo.MaxElapsedTime = maxWait

	var absFileName string
	var err error

	err = func() error {
		if err = backoff.Retry(func() error {
			absFileName, err = fu(ctx, fileName)
			return err
		}, boCtx); err != nil {
			if bo.NextBackOff() == backoff.Stop {
				return err
			}
		}
		return nil
	}()

	return absFileName, err
}

func uploadWithExpBackoff(ctx context.Context, fu func(ctx context.Context, uploadFileAbsPath, actualFileName, absStatusTrackerFileName string) error, uploadFileAbsPath, actualFileName, absStatusTrackerFileName string) error {
	pkgLogger.Debugf("uploading cleaned file with exponential backoff")

	maxWait := time.Minute * 10
	bo := backoff.NewExponentialBackOff()
	boCtx := backoff.WithContext(bo, ctx)
	bo.MaxInterval = time.Minute
	bo.MaxElapsedTime = maxWait

	if err := backoff.Retry(func() error {
		err := fu(ctx, uploadFileAbsPath, actualFileName, absStatusTrackerFileName)
		return err
	}, boCtx); err != nil {
		if bo.NextBackOff() == backoff.Stop {
			return err
		}
	}

	return nil
}

// replace old json.gz & statusTrackerFile with the new during upload.
// Note: upload happens concurrently in 5 go routine by default
func (b *Batch) upload(_ context.Context, uploadFileAbsPath, actualFileName, absStatusTrackerFileName string) error {
	pkgLogger.Debugf("uploading file")
	fileNamePrefixes := strings.Split(actualFileName, "/")

	uploadFilePtr, err := os.OpenFile(uploadFileAbsPath, os.O_RDONLY, 0o644)
	if err != nil {
		return fmt.Errorf("error while opening file, %w", err)
	}
	defer uploadFilePtr.Close()
	_, err = b.FM.Upload(context.TODO(), uploadFilePtr, fileNamePrefixes[1:len(fileNamePrefixes)-1]...)
	if err != nil {
		return fmt.Errorf("error while uploading cleaned file: %w", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	err = b.updateStatusTrackerFile(absStatusTrackerFileName, actualFileName)
	if err != nil {
		return fmt.Errorf("error while updating status tracker file, %w", err)
	}

	statusTrackerFilePtr, err := os.OpenFile(absStatusTrackerFileName, os.O_RDONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("error while opening file, %w", err)
	}
	defer statusTrackerFilePtr.Close()

	_, err = b.FM.Upload(context.TODO(), statusTrackerFilePtr)
	if err != nil {
		return fmt.Errorf("error while uploading statusTrackerFile file: %w", err)
	}

	return nil
}

type BatchManager struct {
	FilesLimit int
	FMFactory  filemanager.Factory
}

func (*BatchManager) GetSupportedDestinations() []string {
	if config.Default.GetBool("REGULATION_WORKER_BATCH_DESTINATIONS_ENABLED", false) {
		return supportedDestinations
	}
	return nil
}

// Delete users corresponding to input userAttributes from a given batch destination
func (bm *BatchManager) Delete(
	ctx context.Context,
	job model.Job,
	destDetail model.Destination,
) model.JobStatus {
	destConfig := destDetail.Config
	destName := destDetail.Name

	pkgLogger.Debugf("deleting job: %v", job, "from batch destination: %v", destName)

	fm, err := bm.FMFactory(&filemanager.Settings{Provider: destName, Config: destConfig})
	if err != nil {
		pkgLogger.Errorf("fetching file manager for destination: %s,  %w", destName, err)
		return model.JobStatus{Status: model.JobStatusAborted, Error: err}
	}

	// parent directory of all the temporary files created/downloaded in the process of deletion.
	baseDIR, err := os.MkdirTemp("", "")
	if err != nil {
		pkgLogger.Errorf("error while creating temporary directory to store all temporary files during deletion: %v", err)
		return model.JobStatus{Status: model.JobStatusFailed, Error: err}
	}

	batch := Batch{
		FM:         fm,
		TmpDirPath: baseDIR,
	}

	prefix := ""
	if val, ok := destConfig["prefix"]; ok {
		prefix = val.(string)
	}
	// Get the prefix which should be the base of the
	// of the cleanup operations.
	defer batch.cleanup(ctx, prefix)

	for {
		files, err := batch.listFiles(ctx, prefix, bm.FilesLimit)
		if err != nil {
			pkgLogger.Errorf("error while getting files list: %v", err)
			return model.JobStatus{Status: model.JobStatusFailed, Error: err}
		}

		if len(files) == 0 {
			pkgLogger.Info("no new files found, breaking")
			break
		}

		fName, err := batch.download(ctx, filepath.Join(prefix, StatusTrackerFileName))
		if err != nil {
			return model.JobStatus{Status: model.JobStatusFailed, Error: err}
		}

		cleanedFiles, err := batch.cleanedFiles(ctx, fName, &job)
		if err != nil {
			pkgLogger.Errorf("error while getting status tracker file: %v", err)
			return model.JobStatus{Status: model.JobStatusFailed, Error: err}
		}

		if len(cleanedFiles) != 0 {
			files = removeCleanedFiles(files, cleanedFiles)
		}

		g, gCtx := errgroup.WithContext(ctx)

		goRoutineCount := make(chan bool, maxRoutines())
		defer close(goRoutineCount)

		for i := 0; i < len(files); i++ {

			_i := i
			goRoutineCount <- true
			g.Go(func() error {
				defer func() {
					<-goRoutineCount
				}()

				// Get filehandler from a factory on every iteration, to not share the data.
				filehandler := LocalFileHandlerFactory(destName, files[_i].Key)
				if filehandler == nil {
					pkgLogger.Warnf("unable to locate filehandler for file: %s under destination: %s", files[_i].Key, destName)
					return nil
				}

				cleanTime := stats.Default.NewTaggedStat(
					"regulation_worker_file_cleaning_time",
					stats.TimerType,
					stats.Tags{
						"destinationId": job.DestinationID,
						"workspaceId":   job.WorkspaceID,
						"jobType":       "batch",
					})
				defer cleanTime.RecordDuration()()

				absPath, err := downloadWithExpBackoff(gCtx, batch.download, files[_i].Key)
				if err != nil {
					return fmt.Errorf("error: %w, while downloading file:%s", err, files[_i].Key)
				}

				fileSizeStat := stats.Default.NewTaggedStat("regulation_worker_file_size_mb", stats.CountType, stats.Tags{"jobId": fmt.Sprintf("%d", job.ID)})
				fileSizeStat.Count(getFileSize(absPath))

				if err := handleIdentityRemoval(ctx, filehandler, job.Users, absPath, absPath); err != nil {
					return fmt.Errorf("unable to handle identity removal for destination: %s, on file: %s, err: %w ", destName, files[_i].Key, err)
				}

				// TODO: Why not have a common function to upload the cleaned files to tracker in one shot ?
				// Why do it one entry at a time ?
				err = uploadWithExpBackoff(gCtx, batch.upload, absPath, files[_i].Key, fName)
				if err != nil {
					return fmt.Errorf("error: %w, while uploading cleaned file:%s", err, files[_i].Key)
				}

				return nil
			})
		}
		err = g.Wait()
		if err != nil {
			pkgLogger.Errorf("user identity deletion job failed with error: %v", err)
			return model.JobStatus{Status: model.JobStatusFailed, Error: err}
		}

		pkgLogger.Infof("successfully completed loop of ")
	}

	return model.JobStatus{Status: model.JobStatusComplete}
}

func LocalFileHandlerFactory(dest, upstreamFilePath string) filehandler.LocalFileHandler {
	switch dest {
	case "S3":
		if strings.HasSuffix(upstreamFilePath, ".json.gz") {
			return filehandler.NewGZIPLocalFileHandler(filehandler.CamelCase)
		}

	case "S3_DATALAKE":
		if strings.HasSuffix(upstreamFilePath, ".parquet") {
			return filehandler.NewParquetLocalFileHandler()
		}

		if strings.HasSuffix(upstreamFilePath, ".json.gz") {
			return filehandler.NewGZIPLocalFileHandler(filehandler.SnakeCase)
		}
	}

	return nil
}

// handleIdentityRemoval is a convenience wrapper over the filehandler
// performing the operations over the file to remove the user identity.
func handleIdentityRemoval(
	ctx context.Context,
	handler filehandler.LocalFileHandler,
	attributes []model.User,
	sourceFile, targetFile string,
) error {
	pkgLogger.Debugf("Handling identity removal for source: %s, destination: %s", sourceFile, targetFile)

	if err := handler.Read(ctx, sourceFile); err != nil {
		return fmt.Errorf("parsing contents of local file: %s, err: %w", sourceFile, err)
	}

	if err := handler.RemoveIdentity(ctx, attributes); err != nil {
		return fmt.Errorf("handle identity removal for attributes: %v, err: %w", nil, err)
	}

	if err := handler.Write(ctx, targetFile); err != nil {
		return fmt.Errorf("writing to local file: %s, err: %w", targetFile, err)
	}

	return nil
}

func maxRoutines() int {
	return 8 * runtime.GOMAXPROCS(0)
}

func getFileSize(fileAbsPath string) int {
	filePtr, _ := os.OpenFile(fileAbsPath, os.O_RDWR, 0o644)
	defer filePtr.Close()
	fileStat, _ := filePtr.Stat()
	fileSize := fileStat.Size() / 1000000
	return int(fileSize)
}

func (b *Batch) cleanup(ctx context.Context, prefix string) {
	pkgLogger.Debugf("cleaning up temp files created during the operation")

	err := b.FM.Delete(
		ctx,
		[]string{filepath.Join(prefix, StatusTrackerFileName)},
	)
	if err != nil {
		pkgLogger.Errorf("error while deleting delete status tracker file from destination: %v", err)
	}

	err = os.RemoveAll(b.TmpDirPath)
	if err != nil {
		pkgLogger.Errorf("error while deleting temporary directory locally: %v", err)
	}
}

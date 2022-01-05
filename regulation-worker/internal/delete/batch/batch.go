package batch

//This is going to call appropriate method of Filemanager & DeleteManager
//to get deletion done.
//called by delete/deleteSvc with (model.Job, model.Destination).
//returns final status,error ({successful, failure}, err)
import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	_ "go.uber.org/automaxprocs"
	"golang.org/x/sync/errgroup"
)

var (
	pkgLogger             = logger.NewLogger().Child("batch")
	regexRequiredSuffix   = regexp.MustCompile(".json.gz$")
	statusTrackerFileName = "ruddderDeleteTracker.txt"
	supportedDestinations = []string{"S3"}
)

const listMaxItem int64 = 1000

type deleteManager interface {
	delete(ctx context.Context, patternFilePtr, targetFilePtr string) ([]byte, error)
}

type Batch struct {
	mu         sync.Mutex
	FM         filemanager.FileManager
	DM         deleteManager
	TmpDirPath string
}

//return appropriate deleteManger based on destination Name
func getDeleteManager(destName string) (*S3DeleteManager, error) {
	switch destName {
	case "S3":
		return &S3DeleteManager{}, nil
	default:
		return nil, model.ErrDestNotImplemented
	}
}

//returns list of all .json.gz files.
//NOTE: assuming that all of batch destination have same file system as S3, i.e. flat.
func (b *Batch) listFiles(ctx context.Context) ([]*filemanager.FileObject, error) {
	pkgLogger.Debugf("getting a list of files from destination")
	fileObjects, err := b.FM.ListFilesWithPrefix("", listMaxItem)
	if err != nil {
		pkgLogger.Errorf("error while getting list of files: %v", err)
		return []*filemanager.FileObject{}, fmt.Errorf("failed to fetch object list from S3: %v", err)
	}
	if len(fileObjects) == 0 {
		return nil, nil
	}

	//since everything is stored as a file in S3, above fileObjects list also has directory & not just *.json.gz files. So, need to remove those.
	count := 0
	for i := 0; i < len(fileObjects); i++ {
		if regexRequiredSuffix.Match([]byte(fileObjects[i].Key)) {
			count++
		}
	}
	//list of only .gz files
	gzFileObjects := make([]*filemanager.FileObject, count)
	index := 0
	for i := 0; i < len(fileObjects); i++ {
		if regexRequiredSuffix.Match([]byte(fileObjects[i].Key)) {
			gzFileObjects[index] = fileObjects[i]
			index++
		}
	}
	return gzFileObjects, nil
}

//two pointer algorithm implementation to remove all the files from which users are already deleted.
func removeCleanedFiles(files []*filemanager.FileObject, cleanedFiles []string) []*filemanager.FileObject {
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
	finalFiles := make([]*filemanager.FileObject, len(files)-presentCount)

	for i := 0; i < len(files); i++ {
		if !present[i] {
			finalFiles[j] = files[i]
			j++
		}
	}
	return finalFiles
}

//append <fileName> to <statusTrackerFile> locally for which deletion has completed.
func (b *Batch) updateStatusTrackerFile(absStatusTrackerFileName, fileName string) error {

	statusTrackerPtr, err := os.OpenFile(absStatusTrackerFileName, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("error while opening file, %w", err)
	}
	defer statusTrackerPtr.Close()

	if _, err := io.WriteString(statusTrackerPtr, fileName+"\n"); err != nil {
		err = fmt.Errorf("error while writing to statusTrackerFile: %w", err)
		return err
	}
	return nil
}

//downloads `fileName` locally. And returns empty file, if file not found.
//Note: download happens concurrently in 5 go routine by default.
func (b *Batch) download(ctx context.Context, completeFileName string) (string, error) {
	pkgLogger.Debugf("downloading file: %v", completeFileName)

	tmpFilePathPrefix, err := os.MkdirTemp(b.TmpDirPath, "")
	if err != nil {
		pkgLogger.Errorf("error while creating temporary directory: %v", err)
		return "", fmt.Errorf("error while creating temporary directory: %w", err)
	}
	_, fileName := filepath.Split(completeFileName)
	tmpFilePtr, err := os.OpenFile(filepath.Join(tmpFilePathPrefix, fileName), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		pkgLogger.Errorf("error while opening file, %v", err)
		return "", fmt.Errorf("error while opening file, %w", err)
	}
	defer tmpFilePtr.Close()
	absPath, err := filepath.Abs(tmpFilePtr.Name())
	if err != nil {
		pkgLogger.Errorf("error while getting absolute path: %v", err)
		return "", fmt.Errorf("error while getting absolute path: %w", err)
	}
	err = b.FM.Download(tmpFilePtr, completeFileName)
	if err != nil {
		if err == filemanager.ErrKeyNotFound {
			pkgLogger.Debugf("file not found")
			return absPath, nil
		}
		pkgLogger.Errorf("error while downloading object using file manager: %v", err)
		return "", fmt.Errorf("error while downloading object using file manager: %w", err)
	}
	return absPath, nil
}

//decompresses .json.gzip files to .json & remove corresponding .json.gzip file
func (b *Batch) decompress(compressedFileName string) (string, error) {

	compressedFilePtr, err := os.OpenFile(compressedFileName, os.O_RDWR, 0644)
	if err != nil {
		return "", fmt.Errorf("error while opening compressed file, %w", err)
	}
	defer compressedFilePtr.Close()

	gzipReader, err := gzip.NewReader(compressedFilePtr)
	if err != nil {
		return "", fmt.Errorf("error while reading compressed file: %w", err)
	}

	decompressedFilePtr, err := os.CreateTemp(b.TmpDirPath, "")
	if err != nil {
		return "", fmt.Errorf("error while creating temporary file for decompressed files during cleaning")
	}
	defer decompressedFilePtr.Close()

	_, err = io.Copy(decompressedFilePtr, gzipReader)
	if err != nil {
		return "", fmt.Errorf("error while writing uncompressed file: %w", err)
	}

	decompressedFileName, err := filepath.Abs(decompressedFilePtr.Name())
	if err != nil {
		return "", err
	}

	return decompressedFileName, nil
}

//compress & write `cleanedBytes` of type []byte to `fileName`
func (b *Batch) compress(fileName string, cleanedBytes []byte) error {

	//compressing
	var buffer bytes.Buffer
	w := gzip.NewWriter(&buffer)
	_, err := w.Write([]byte(cleanedBytes))
	if err != nil {
		return fmt.Errorf("error while compressing file: %w", err)
	}
	w.Close() // must close this first to flush the bytes to the buffer.

	cleanCompressedFilePtr, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("error while opening file, %w", err)
	}
	defer cleanCompressedFilePtr.Close()

	_, err = cleanCompressedFilePtr.Write(buffer.Bytes())
	if err != nil {
		return fmt.Errorf("error while writing cleaned & compressed data:%w", err)
	}

	return nil
}

// delete users corresponding to `userAttributes` from `fileName` available locally
func (b *Batch) delete(ctx context.Context, PatternFile, targetFile string) error {

	decompressedFile, err := b.decompress(targetFile)
	if err != nil {
		return fmt.Errorf("error while decompressing file: %w", err)
	}

	out, err := b.DM.delete(ctx, PatternFile, decompressedFile)
	if err != nil {
		return fmt.Errorf("error while cleaning object, %w", err)
	}

	err = b.compress(targetFile, out)
	if err != nil {
		return fmt.Errorf("error while compressing file: %w", err)
	}

	return nil
}

func downloadWithExpBackoff(ctx context.Context, fu func(context.Context, string) (string, error), fileName string) (string, error) {
	pkgLogger.Debugf("downloading file with exponential backoff")
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

//replace old json.gz & statusTrackerFile with the new during upload.
//Note: upload happens concurrently in 5 go routine by default
func (b *Batch) upload(ctx context.Context, uploadFileAbsPath, actualFileName, absStatusTrackerFileName string) error {
	pkgLogger.Debugf("uploading file")
	fileNamePrefixes := strings.Split(actualFileName, "/")

	uploadFilePtr, err := os.OpenFile(uploadFileAbsPath, os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("error while opening file, %w", err)
	}
	defer uploadFilePtr.Close()

	_, err = b.FM.Upload(uploadFilePtr, fileNamePrefixes[1:len(fileNamePrefixes)-1]...)
	if err != nil {
		return fmt.Errorf("error while uploading cleaned file: %w", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	err = b.updateStatusTrackerFile(absStatusTrackerFileName, actualFileName)
	if err != nil {
		return fmt.Errorf("error while updating status tracker file, %w", err)
	}

	statusTrackerFilePtr, err := os.OpenFile(absStatusTrackerFileName, os.O_RDONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("error while opening file, %w", err)
	}
	defer statusTrackerFilePtr.Close()

	_, err = b.FM.Upload(statusTrackerFilePtr)
	if err != nil {
		return fmt.Errorf("error while uploading statusTrackerFile file: %w", err)
	}

	return nil
}

func (b *Batch) createPatternFile(userAttributes []model.UserAttribute) (string, error) {
	pkgLogger.Debug("creating a file with pattern to be searched & deleted.")

	searchObject := make([]byte, 0)

	for _, users := range userAttributes {
		searchObject = append(searchObject, "/"...)
		searchObject = append(searchObject, "\"userId\": *\""...)
		searchObject = append(searchObject, users.UserID...)
		searchObject = append(searchObject, "\"/d;"...)

		if users.Email != nil {
			searchObject = append(searchObject, "/"...)
			searchObject = append(searchObject, "\"email\": *\""...)
			searchObject = append(searchObject, []byte(*users.Email)...)
			searchObject = append(searchObject, "\"/d;"...)
		}

		if users.Phone != nil {
			searchObject = append(searchObject, "/"...)
			searchObject = append(searchObject, "\"phone\": *\""...)
			searchObject = append(searchObject, []byte(*users.Phone)...)
			searchObject = append(searchObject, "\"/d;"...)
		}
	}

	PatternFilePtr, err := os.CreateTemp(b.TmpDirPath, "")
	if err != nil {
		pkgLogger.Errorf("error while creating pattern file: %v", err)
		return "", fmt.Errorf("error while creating patternFile: %w", err)
	}
	defer PatternFilePtr.Close()

	_, err = PatternFilePtr.Write(searchObject)
	if err != nil {
		pkgLogger.Errorf("error while writing pattern: %v", err)
		return "", fmt.Errorf("error while writing pattern:%w", err)
	}
	absPatternFile, err := filepath.Abs(PatternFilePtr.Name())

	return absPatternFile, err
}

type BatchManager struct {
}

type KVDeleteManager struct {
}

func (bm *BatchManager) GetSupportedDestinations() []string {

	return supportedDestinations
}

//TODO: aws s3 ListObject allows listing of at max 1000 object at a time. So, implement paginatin.
//Delete users corresponding to input userAttributes from a given batch destination
func (bm *BatchManager) Delete(ctx context.Context, job model.Job, destConfig map[string]interface{}, destName string) model.JobStatus {
	pkgLogger.Debugf("deleting job: %v", job, "from batch destination: %v", destName)

	fmFactory := filemanager.FileManagerFactoryT{}
	fm, err := fmFactory.New(&filemanager.SettingsT{
		Provider: destName,
		Config:   destConfig,
	})
	if err != nil {
		pkgLogger.Errorf("error while getting file manager: %v", err)
		return model.JobStatusFailed
	}

	dm, err := getDeleteManager(destName)
	if err != nil {
		pkgLogger.Errorf("error while getting appropriate delete manager: %v", err)
		return model.JobStatusFailed
	}

	//parent directory of all the temporary files created/downloaded in the process of deletion.
	tmpDirPath, err := os.MkdirTemp("", "")
	if err != nil {
		pkgLogger.Errorf("error while creating temporary directory to store all temporary files during deletion: %v", err)
		return model.JobStatusFailed
	}

	batch := Batch{
		FM:         fm,
		DM:         dm,
		TmpDirPath: tmpDirPath,
	}
	defer batch.cleanup(destConfig["prefix"].(string))

	//file with pattern to be searched & deleted from all downloaded files.
	absPatternFile, err := batch.createPatternFile(job.UserAttributes)
	if err != nil {
		pkgLogger.Errorf("error while creating pattern file: %v", err)
		return model.JobStatusFailed
	}

	for {
		files, err := batch.listFiles(ctx)
		if err != nil {
			pkgLogger.Errorf("error while getting files list: %v", err)
			return model.JobStatusFailed
		}

		if len(files) == 0 {
			pkgLogger.Debug("no new files found")
			break
		}
		//if statusTracker.txt exists then read it & remove all those files name from above gzFilesObjects,
		//since those files are already cleaned.
		var cleanedFiles []string
		absStatusTrackerFileName, err := func() (string, error) {
			absStatusTrackerFileName, err := batch.download(ctx, filepath.Join(destConfig["prefix"].(string), statusTrackerFileName))
			if err != nil {
				pkgLogger.Errorf("error while downloading statusTrackerFile: %v", err)
				return "", fmt.Errorf("error while downloading statusTrackerFile: %w", err)
			}

			statusTrackerFilePtr, err := os.OpenFile(absStatusTrackerFileName, os.O_RDWR, 0644)
			if err != nil {
				pkgLogger.Errorf("error while opening file, %v", err)
				return "", fmt.Errorf("error while opening file, %w", err)
			}
			defer statusTrackerFilePtr.Close()

			data, err := io.ReadAll(statusTrackerFilePtr)
			if err != nil {
				pkgLogger.Errorf("error while reading statusTrackerFile: %v", err)
				return "", fmt.Errorf("error while reading statusTrackerFile: %w", err)
			}
			//if statusTracker.txt exists then read it & remove all those files name from above gzFilesObjects,
			//since those files are already cleaned.

			jobID := fmt.Sprintf("%d", job.ID)

			if len(data) == 0 {
				//insert <jobID> in 1st line
				if _, err := io.WriteString(statusTrackerFilePtr, jobID+"\n"); err != nil {
					pkgLogger.Errorf("error while writing jobId to statusTrackerFile: %v", err)
					return "", fmt.Errorf("error while writing to jobId to statusTrackerFile: %w", err)
				}
			} else {
				lines := strings.Split(string(data), "\n")
				//check if our <jobID> matches with the one in file.
				//if not, then truncate the file & write new current jobID.
				if lines[0] != jobID {
					_ = statusTrackerFilePtr.Close()
					statusTrackerTmpDir, err := os.MkdirTemp(batch.TmpDirPath, "")
					if err != nil {
						pkgLogger.Errorf("error while creating temporary directory: %v", err)
						return "", fmt.Errorf("error while creating temporary directory: %w", err)
					}
					statusTrackerFilePtr, err = os.OpenFile(filepath.Join(statusTrackerTmpDir, statusTrackerFileName), os.O_CREATE|os.O_RDWR, 0644)
					if err != nil {
						pkgLogger.Errorf("error while opening file, %v", err)
						return "", fmt.Errorf("error while opening file, %w", err)
					}

					if _, err := io.WriteString(statusTrackerFilePtr, jobID+"\n"); err != nil {
						pkgLogger.Errorf("error while writing to statusTrackerFile: %v", err)
						err = fmt.Errorf("error while writing to statusTrackerFile: %w", err)
						return "", err
					}
				} else {
					//if yes, then simply read it.
					cleanedFiles = lines[1 : len(lines)-1]
				}
			}
			absPath, err := filepath.Abs(statusTrackerFilePtr.Name())

			return absPath, err
		}()
		if err != nil {
			pkgLogger.Errorf("error while getting status tracker file: %v", err)
			return model.JobStatusFailed
		}
		if len(cleanedFiles) != 0 {
			files = removeCleanedFiles(files, cleanedFiles)
		}

		g, gCtx := errgroup.WithContext(ctx)

		procAllocated, err := strconv.Atoi(config.GetEnv("GOMAXPROCS", "32"))
		if err != nil {
			pkgLogger.Errorf("error while getting maximum number of go routines to be created: %v", err)
			return model.JobStatusFailed
		}
		maxGoRoutine := 8 * procAllocated
		pkgLogger.Debugf("maximum number of go routines that can be created: %w", maxGoRoutine)
		goRoutineCount := make(chan bool, maxGoRoutine)
		defer close(goRoutineCount)

		for i := 0; i < len(files); i++ {
			_i := i
			goRoutineCount <- true
			g.Go(func() error {
				//TODO: add file size stats
				fileCleaningTime := stats.NewTaggedStat("file_cleaning_time", stats.TimerType, stats.Tags{"jobId": fmt.Sprintf("%d", job.ID), "workspaceId": job.WorkspaceID, "destType": "batch", "destName": destName})
				fileCleaningTime.Start()

				defer func() {
					fileCleaningTime.End()
					<-goRoutineCount
				}()

				FileAbsPath, err := downloadWithExpBackoff(gCtx, batch.download, files[_i].Key)
				if err != nil {
					pkgLogger.Errorf("error: %v, while downloading file: %v", err, files[_i].Key)
					return fmt.Errorf("error: %w, while downloading file:%s", err, files[_i].Key)
				}

				fileSizeStats := stats.NewTaggedStat("file_size_mb", stats.CountType, stats.Tags{"jobId": fmt.Sprintf("%d", job.ID)})
				fileSizeStats.Count(getFileSize(FileAbsPath))

				getFileSize(FileAbsPath)
				err = batch.delete(gCtx, absPatternFile, FileAbsPath)
				if err != nil {
					pkgLogger.Errorf("error: %v, while deleting file:%v", err, files[_i].Key)
					return fmt.Errorf("error: %w, while deleting file:%s", err, files[_i].Key)
				}

				err = uploadWithExpBackoff(gCtx, batch.upload, FileAbsPath, files[_i].Key, absStatusTrackerFileName)
				if err != nil {
					pkgLogger.Errorf("error: %v, while uploading cleaned file:%v", err, files[_i].Key)
					return fmt.Errorf("error: %w, while uploading cleaned file:%s", err, files[_i].Key)
				}

				return nil
			})
		}
		err = g.Wait()
		if err != nil {
			pkgLogger.Errorf("job failed with error: %v", err)
			return model.JobStatusFailed
		}
	}
	return model.JobStatusComplete
}

func getFileSize(fileAbsPath string) int {
	filePtr, _ := os.OpenFile(fileAbsPath, os.O_RDWR, 0644)
	defer filePtr.Close()
	fileStat, _ := filePtr.Stat()
	fileSize := fileStat.Size() / 1000000
	return int(fileSize)
}

func (b *Batch) cleanup(prefix string) {
	pkgLogger.Debugf("removing all temporary files & directory locally & from destination.")
	err := b.FM.DeleteObjects([]string{filepath.Join(prefix, statusTrackerFileName)})
	if err != nil {
		pkgLogger.Errorf("error while deleting delete status tracker file from destination: %v", err)
	}
	err = os.RemoveAll(b.TmpDirPath)
	if err != nil {
		pkgLogger.Errorf("error while deleting temporary directory locally: %v", err)
	}
}

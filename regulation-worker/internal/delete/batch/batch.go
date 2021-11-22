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
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"golang.org/x/sync/errgroup"
)

const statusTrackerFile = "deleteStatusTracker.txt"
const listMaxItem int64 = 1000

var maxGoRoutine = int64(runtime.NumCPU()) * 8

type deleteManager interface {
	delete(ctx context.Context, userAttributes []model.UserAttribute, fileName string) ([]byte, error)
}

type Batch struct {
	mutex sync.Mutex
	FM    filemanager.FileManager
	DM    deleteManager
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

//returns list of all .json.gz files and marks exists as true if `statusTrackerFile` is present in the destination.
//NOTE: assuming that all of batch destination have same file system as S3, i.e. flat.
func (b *Batch) listFilesAndCheckTrackerFile(ctx context.Context) ([]*filemanager.FileObject, bool, error) {

	fileObjects, err := b.FM.ListFilesWithPrefix("", listMaxItem)
	if err != nil {
		return []*filemanager.FileObject{}, false, fmt.Errorf("failed to fetch object list from S3:%w", err)
	}

	//since everything is stored as a file in S3, above fileObjects list also has directory & not just *.json.gz files. So, need to remove those.
	count := 0
	exist := false
	regexRequiredSuffix := regexp.MustCompile(".json.gz$")
	for i := 0; i < len(fileObjects); i++ {
		if regexRequiredSuffix.Match([]byte(fileObjects[i].Key)) {
			count++
		}
		if fileObjects[i].Key == statusTrackerFile {
			exist = true
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
	return gzFileObjects, exist, nil
}

//two pointer algorithm implementation to remove all the files from which users are already deleted.
func removeCleanedFiles(files []*filemanager.FileObject, cleanedFiles []string) []*filemanager.FileObject {
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
func updateStatusTrackerFile(fileName string) error {
	f, err := os.OpenFile(statusTrackerFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error while opening statusTrackerFile: %w", err)
	}
	if _, err := f.Write([]byte(fileName)); err != nil {
		return fmt.Errorf("error while writing to statusTrackerFile: %w", err)
	}
	if _, err := f.Write([]byte("\n")); err != nil {
		return fmt.Errorf("error while writing to statusTrackerFile: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("error while closing statusTrackerFile: %w", err)
	}
	return nil
}

//downloads `fileName` locally
//Note: download happens concurrently in 5 go routine by default
func (b *Batch) download(ctx context.Context, fileName string) error {
	fileNamePrefix := strings.Split(fileName, "/")
	filePtr, err := os.OpenFile(fileNamePrefix[len(fileNamePrefix)-1], os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("error while opening file, %w", err)
	}
	defer filePtr.Close()
	err = b.FM.Download(filePtr, fileName)
	if err != nil {
		return fmt.Errorf("error while downloading object using file manager, %w", err)
	}
	return nil
}

//decompresses .json.gzip files to .json & remove corresponding .json.gzip file
func decompress(fileName, decompressedFileName string) error {
	gzipFile, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("error while opening compressed file: %w", err)
	}

	gzipReader, err := gzip.NewReader(gzipFile)
	if err != nil {
		return fmt.Errorf("error while reading compressed file: %w", err)
	}
	defer gzipReader.Close()

	outfileWriter, err := os.OpenFile(decompressedFileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("error while opening uncompressed file: %w", err)
	}
	defer outfileWriter.Close()

	_, err = io.Copy(outfileWriter, gzipReader)
	if err != nil {
		return fmt.Errorf("error while writing uncompressed file: %w", err)
	}

	os.Remove(fileName)
	return nil
}

//compress & write `cleanedBytes` of type []byte to `fileName`
func compress(fileName string, cleanedBytes []byte) error {

	//compressing
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	_, err := w.Write([]byte(cleanedBytes))
	if err != nil {
		return fmt.Errorf("error while compressing file: %w", err)
	}
	w.Close() // must close this first to flush the bytes to the buffer.

	//writing compressed file to <fileName>
	outfileWriter, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("error while opening file: %w", err)
	}
	defer outfileWriter.Close()
	_, err = outfileWriter.Write(b.Bytes())
	if err != nil {
		return fmt.Errorf("error while writing cleaned & compressed data:%w", err)
	}
	return nil
}

// delete users corresponding to `userAttributes` from `fileName` available locally
func (b *Batch) delete(ctx context.Context, userAttributes []model.UserAttribute, fileName string) error {

	decompressedFileName := "decompressed_" + fileName
	err := decompress(fileName, decompressedFileName)
	if err != nil {
		return fmt.Errorf("error while decompressing file: %w", err)
	}

	out, err := b.DM.delete(ctx, userAttributes, decompressedFileName)
	if err != nil {
		return fmt.Errorf("error while cleaning object, %w", err)
	}

	err = compress(fileName, out)
	if err != nil {
		return fmt.Errorf("error while compressing file: %w", err)
	}
	return nil
}

func withExpBackoff(fu func(context.Context, string) error, ctx context.Context, fileName string) error {

	maxWait := time.Minute * 10
	bo := backoff.NewExponentialBackOff()
	boCtx := backoff.WithContext(bo, ctx)
	bo.MaxInterval = time.Minute
	bo.MaxElapsedTime = maxWait

	if err := backoff.Retry(func() error {
		err := fu(ctx, fileName)
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
func (b *Batch) upload(ctx context.Context, fileName string) error {

	fileNamePrefix := strings.Split(fileName, "/")
	cleanedFile := fileNamePrefix[len(fileNamePrefix)-1]
	//as 0th index is correspondnig to `manager.Config.Prefix`
	fileNamePrefix = fileNamePrefix[1 : len(fileNamePrefix)-1]

	file, err := os.OpenFile(cleanedFile, os.O_RDONLY, os.FileMode(int(0777)))
	if err != nil {
		return fmt.Errorf("error while opening cleaned file for uploading: %w", err)
	}

	_, err = b.FM.Upload(file, fileNamePrefix...)
	if err != nil {
		return fmt.Errorf("error while uploading cleaned file: %w", err)
	}
	file.Close()

	b.mutex.Lock()
	err = updateStatusTrackerFile(fileName)
	if err != nil {
		return fmt.Errorf("error while updating status tracker file, %w", err)
	}

	file, err = os.OpenFile(statusTrackerFile, os.O_RDONLY, os.FileMode(int(0777)))
	if err != nil {
		return fmt.Errorf("error while opening statusTrackerFile file for uploading: %w", err)
	}
	defer file.Close()

	_, err = b.FM.Upload(file)
	if err != nil {
		return fmt.Errorf("error while uploading statusTrackerFile file: %w", err)
	}
	b.mutex.Unlock()
	os.Remove(cleanedFile)
	return nil
}

//TODO: aws s3 ListObject allows listing of at max 1000 object at a time. So, implement paginatin.
//Delete users corresponding to input userAttributes from a given batch destination
func Delete(ctx context.Context, job model.Job, destConfig map[string]interface{}, destName string) error {
	fmFactory := filemanager.FileManagerFactoryT{}
	fm, err := fmFactory.New(&filemanager.SettingsT{
		Provider: destName,
		Config:   destConfig,
	})
	if err != nil {
		return fmt.Errorf("error while creating file manager: %w", err)
	}

	dm, err := getDeleteManager(destName)
	if err != nil {
		return fmt.Errorf("failed to get appropriate deleteManager, %w", err)
	}

	batch := Batch{
		FM: fm,
		DM: dm,
	}
	defer batch.cleanup(destConfig["prefix"].(string))
	files, exist, err := batch.listFilesAndCheckTrackerFile(ctx)
	if err != nil {
		return err
	}
	//if statusTracker.txt exists then read it & remove all those files name from above gzFilesObjects,
	//since those files are already cleaned.
	if exist {

		err = batch.download(ctx, statusTrackerFile)
		if err != nil {
			return fmt.Errorf("error while downloading statusTrackerFile:%w", err)
		}

		data, err := os.ReadFile(statusTrackerFile)
		if err != nil {
			return fmt.Errorf("error while reading statusTracker.txt:%w", err)
		}

		cleanedFiles := strings.Split(string(data), "\n")
		cleanedFiles = cleanedFiles[:len(cleanedFiles)-1]
		files = removeCleanedFiles(files, cleanedFiles)

	} else {
		filePtr, err := os.Create(statusTrackerFile)
		if err != nil {
			return fmt.Errorf("error while creating statusTrackerFile:%w", err)
		}
		filePtr.Close()
	}
	g, gCtx := errgroup.WithContext(ctx)
	goRoutineCount := make(chan bool, maxGoRoutine)
	defer close(goRoutineCount)
	for i := 0; i < len(files); i++ {
		_i := i
		goRoutineCount <- true
		g.Go(func() error {
			err = withExpBackoff(batch.download, gCtx, files[_i].Key)
			if err != nil {
				return err
			}
			fileNamePrefix := strings.Split(files[_i].Key, "/")
			err := batch.delete(gCtx, job.UserAttributes, fileNamePrefix[len(fileNamePrefix)-1])
			if err != nil {
				return fmt.Errorf("error while deleting object, %w", err)
			}
			err = withExpBackoff(batch.upload, gCtx, files[_i].Key)
			if err != nil {
				return fmt.Errorf("error while uploading cleaned file, %w", err)
			}
			<-goRoutineCount
			return nil
		})
	}
	err = g.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (b *Batch) cleanup(prefix string) {

	// os.Remove(statusTrackerFile)
	err := b.FM.DeleteObjects([]string{prefix + "/" + statusTrackerFile})
	if err != nil {
		fmt.Println("error during cleanup: %w", err)
	}
}

package batch

//This is going to call appropriate method of Filemanager & DeleteManager
//to get deletion done.
//called by delete/deleteSvc with (model.Job, model.Destination).
//returns final status,error ({successful, failure}, err)
import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/filemanager"
)

type deleteManager interface {
	Delete(ctx context.Context, userAttributes []model.UserAttribute, fileName string) (model.JobStatus, error)
}

type Batch struct {
	FileManager   filemanager.FileManager
	DeleteManager deleteManager
}

//returns list of all .json.gz files and marks exists as true if `statusTrackerFile` is present in the s3 bucket.
func ListFiles(fm filemanager.FileManager, statusTrackerFile string) ([]*filemanager.FileObject, bool, error) {
	var maxItem int64 = 1000

	fileObjects, err := fm.ListFilesWithPrefix("", maxItem)
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
			fmt.Println(gzFileObjects[index].Key)
			index++
		}
	}
	fmt.Println("exists=", exist)
	return gzFileObjects, exist, nil
}

//two pointer algorithm implementation to remove all the files from which users are already deleted.
func removeCleanedFiles(files []*filemanager.FileObject, cleanedFiles []string) []*filemanager.FileObject {

	sort.Slice(files, func(i, j int) bool {
		return files[i].Key < files[j].Key
	})

	sort.Slice(cleanedFiles, func(i, j int) bool {
		return files[i].Key < files[j].Key
	})

	finalFiles := make([]*filemanager.FileObject, len(files)-len(cleanedFiles))
	i := 0
	j := 0
	fmt.Println("len of finalFiles=", len(finalFiles))
	present := make([]bool, len(files))
	for j < len(cleanedFiles) {

		if files[i].Key < cleanedFiles[j] {
			i++
		} else if files[i].Key > cleanedFiles[j] {
			j++
		} else {
			present[i] = true
			i++
			j++
		}
	}
	j = 0
	for i := 0; i < len(files); i++ {
		if !present[i] {
			finalFiles[j] = files[i]
			j++
		}
	}
	return finalFiles
}

//calls filemanager to download data
//calls deletemanager to delete users from downloaded data
//calls filemanager to upload data
//TODO: aws s3 ListObject allows listing of at max 1000 object at a time. So, implement paginatin.
//If pagination is not implemented, then cleaning for bucket's object with index>1000 will never be happen.
func (b *Batch) Delete(ctx context.Context, job model.Job, destDetail model.Destination) (status model.JobStatus, err error) {

	//TODO: implement a better way of getting prefix & maxItem.
	statusTrackerFile := "deleteStatusTracker.txt"
	files, exist, err := ListFiles(b.FileManager, statusTrackerFile)
	if err != nil {
		return model.JobStatusFailed, err
	}

	//if statusTracker.txt exists then read it & remove all those files name from above gzFilesObjects,
	//since those files are already cleaned.
	//if statusTracker.txt doesn't exist then create one.
	if exist {
		file, err := os.OpenFile(statusTrackerFile, os.O_CREATE|os.O_RDWR, 0777)
		// file, err := os.Create(statusTrackerFile)
		if err != nil {
			return model.JobStatusFailed, err
		}
		// defer file.Close()
		// os.Truncate(statusTrackerFile, 0)
		b.FileManager.Download(file, statusTrackerFile)
		if err != nil {
			return model.JobStatusFailed, fmt.Errorf("error while downloading statusTracker.txt:%w", err)
		}
		file.Close()
		data, err := os.ReadFile(statusTrackerFile)
		if err != nil {
			return model.JobStatusFailed, fmt.Errorf("error while reading statusTracker.txt:%w", err)
		}
		cleanedFiles := strings.Split(string(data), "\n")
		fmt.Println("Cleaned files list=", cleanedFiles)
		files = removeCleanedFiles(files, cleanedFiles)
	}

	for i := 0; i < len(files); i++ {
		//download file
		file, err := os.OpenFile(files[i].Key, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return model.JobStatusFailed, err
		}
		b.FileManager.Download(file, files[i].Key)

		//delete users from downloaded file
		b.DeleteManager.Delete(ctx, job.UserAttributes, files[i].Key)

		//update statusTrackerFile locally
		updateStatusTrackerFile(statusTrackerFile, files[i].Key)

		//replace old json.gz & statusTrackerFile with the new during upload.

	}
	return model.JobStatusComplete, nil
}

func updateStatusTrackerFile(statusTrackerFile, fileName string) error {
	f, err := os.OpenFile(statusTrackerFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error while opening statusTrackerFile: %w", err)
	}
	if _, err := f.Write([]byte(fileName)); err != nil {
		return fmt.Errorf("error while writing to statusTrackerFile: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("error while closing statusTrackerFile: %w", err)
	}
	return nil
}

//check if statusTracker.txt exists or not. If not, then create one.
//if exists, then load it in one array & remove all files in this list from the one loaded before.
//to do this: sort both files & follow two pointer approach to get the updated list in O(n).

/*
	for _, s3Object := range fileObjects {
		// if s3Object.LastModified.Before(startTime) {
		// 	continue
		// }
		jsonPath := "/Users/srikanth/" + "s3-correctness/" + uuid.Must(uuid.NewV4()).String()
		err = os.MkdirAll(filepath.Dir(jsonPath), os.ModePerm)
		jsonFile, err := os.Create(jsonPath)
		if err != nil {
			panic(err)
		}

		err = s3Manager.Download(jsonFile, s3Object.Key)
		if err != nil {
			panic(err)
		}
		jsonFile.Close()
		defer os.Remove(jsonPath)

		rawf, err := os.Open(jsonPath)
		reader, _ := gzip.NewReader(rawf)

		sc := bufio.NewScanner(reader)

		count := 0
		for sc.Scan() {
			lineBytes := sc.Bytes()
			eventID := gjson.GetBytes(lineBytes, "messageId").String()
			userID := gjson.GetBytes(lineBytes, "anonymousId").String()
			timeStamp := gjson.GetBytes(lineBytes, "timeStamp").String()
			pipe.RPush(testName+":"+userID+":dst_list", eventID)
			pipe.SAdd(redisDestUserSet, userID)
			pipe.SAdd(redisDestEventSet, eventID)
			pipe.HSet(redisDestEventTimeHash, eventID, timeStamp)
			if count%100 == 0 {
				pipe.Exec()
			}
			count++
		}
		pipe.Exec()
		reader.Close()
	}
*/
/*
	sort.Slice(fileObjects, func(i, j int) bool {
		return fileObjects[i].LastModified.Before(fileObjects[j].LastModified)
	})

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisServer,
	})
	pipe := redisClient.Pipeline()

	for _, s3Object := range fileObjects {
		if s3Object.LastModified.Before(startTime) {
			continue
		}
		jsonPath := "/Users/srikanth/" + "s3-correctness/" + uuid.Must(uuid.NewV4()).String()
		err = os.MkdirAll(filepath.Dir(jsonPath), os.ModePerm)
		jsonFile, err := os.Create(jsonPath)
		if err != nil {
			panic(err)
		}

		err = s3Manager.Download(jsonFile, s3Object.Key)
		if err != nil {
			panic(err)
		}
		jsonFile.Close()
		defer os.Remove(jsonPath)

		rawf, err := os.Open(jsonPath)
		reader, _ := gzip.NewReader(rawf)

		sc := bufio.NewScanner(reader)

		count := 0
		for sc.Scan() {
			lineBytes := sc.Bytes()
			eventID := gjson.GetBytes(lineBytes, "messageId").String()
			userID := gjson.GetBytes(lineBytes, "anonymousId").String()
			timeStamp := gjson.GetBytes(lineBytes, "timeStamp").String()
			pipe.RPush(testName+":"+userID+":dst_list", eventID)
			pipe.SAdd(redisDestUserSet, userID)
			pipe.SAdd(redisDestEventSet, eventID)
			pipe.HSet(redisDestEventTimeHash, eventID, timeStamp)
			if count%100 == 0 {
				pipe.Exec()
			}
			count++
		}
		pipe.Exec()
		reader.Close()
	}
*/
// data, err := getData(job, dest)
// if err != nil {
// 	return model.JobStatusFailed, fmt.Errorf("error while getting deletion data: %w", err)
// }
// cleanedData, err := deleteData(job, dest, data)
// if err != nil {
// 	return model.JobStatusFailed, fmt.Errorf("error while deleting users from destination data: %w", err)
// }

// status, err := uploadData(job, dest, cleanedData)
// if err != nil {
// 	return model.JobStatusFailed, fmt.Errorf("error while uploading deleted data: %w", err)
// }
// return b.DeleteManager.Delete(ctx, job, destDetail)

// }

// func getData(job model.Job, dest model.Destination) (interface{}, error) {
// 	return nil, nil
// }

// func deleteData(job model.Job, dest model.Destination, data interface{}) (interface{}, error) {
// 	return nil, nil
// }

// func uploadData(job model.Job, dest model.Destination, data interface{}) (model.JobStatus, error) {
// 	return model.JobStatusComplete, nil
// }

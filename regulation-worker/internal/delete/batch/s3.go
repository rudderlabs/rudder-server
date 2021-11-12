package batch

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

//Given (data,[]userAttributes)
//delete users info from `data` for all users in []userAttributes
//return remaining data.

type S3DeleteManager struct {
}

func decompress(fileName, uncompressedFileName string) {

	gzipFile, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}

	gzipReader, err := gzip.NewReader(gzipFile)
	if err != nil {
		log.Fatal(err)
	}
	defer gzipReader.Close()

	outfileWriter, err := os.OpenFile(uncompressedFileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.FileMode(int(0777)))
	if err != nil {
		log.Fatal(err)
	}
	defer outfileWriter.Close()

	_, err = io.Copy(outfileWriter, gzipReader)
	if err != nil {
		log.Fatal(err)
	}
}

//reason behind using sed: https://www.rtuin.nl/2012/01/fast-search-and-replace-in-large-files-with-sed/
func (dm *S3DeleteManager) Delete(ctx context.Context, userAttributes []model.UserAttribute, fileName string) (model.JobStatus, error) {

	//unzip
	uncompressedFileName := "uncompressedFile2.json"
	fmt.Println("decompress started")
	decompress(fileName, uncompressedFileName)
	fmt.Println("decompress complete")

	//actual delete
	var n int
	for i := 0; i < len(userAttributes); i++ {
		n += len(userAttributes[i].UserID) + 4
		if userAttributes[i].Email != nil {
			n += len(*userAttributes[i].Email) + 4
		}
		if userAttributes[i].Phone != nil {
			n += len(*userAttributes[i].Phone) + 4
		}
	}
	// searchObject = append(searchObject, "'"...)
	/*searchObject := make([]byte, 0, n)
	for _, users := range userAttributes {
		searchObject = append(searchObject, "/"...)
		searchObject = append(searchObject, users.UserID...)
		searchObject = append(searchObject, "/d;"...)

		if users.Email != nil {
			searchObject = append(searchObject, "/"...)
			searchObject = append(searchObject, []byte(*users.Email)...)
			searchObject = append(searchObject, "/d;"...)
		}

		if users.Phone != nil {
			searchObject = append(searchObject, "/"...)
			searchObject = append(searchObject, []byte(*users.Phone)...)
			searchObject = append(searchObject, "/d;"...)
		}
	}
	*/
	searchObject := "/Jermaine1473336609491897794707338/d"
	out, err := exec.Command("sed", "-e", string(searchObject), uncompressedFileName).Output()
	if err != nil {
		fmt.Printf("error while running command: %s", err)
	}
	// cleanedFileName := "cleanedFile-1.json"
	// err = os.WriteFile(cleanedFileName, out, 0644)
	// if err != nil {
	// 	fmt.Printf("%s", err)
	// }

	//gzip the deleted file again
	compress(fileName, out)

	return model.JobStatusComplete, nil
}

func compress(fileName string, cleanedBytes []byte) {

	//compressing
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write([]byte(cleanedBytes))
	w.Close() // must close this first to flush the bytes to the buffer.

	//writing compressed file to <fileName>
	outfileWriter, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Fatal(err)
	}
	defer outfileWriter.Close()
	_, err = outfileWriter.Write(b.Bytes())
	if err != nil {
		fmt.Println("error while writing cleaned & compressed data:", err)
	}

}

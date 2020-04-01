package helpers

import (
	"cloud.google.com/go/storage"
	"context"
	"io"
	"os"
	"strings"
)

func getObjectsIterator(bucket string, prefix string) *storage.ObjectIterator {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		panic(err)
	}
	it := client.Bucket(bucket).Objects(ctx, &storage.Query{
		Prefix: prefix,
	})
	return it
}

func DownloadObject(location string, bucket string, file *os.File) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		panic(err)
	}
	gcsBaseUrl := "https://storage.googleapis.com/"
	location = strings.Replace(location, gcsBaseUrl+bucket+"/", "", 1)
	objReader, err := client.Bucket(bucket).Object(location).NewReader(ctx)

	if err != nil {
		panic(err)
	}
	defer objReader.Close()
	_, err = io.Copy(file, objReader)
	if err != nil {
		panic(err)
	}
}

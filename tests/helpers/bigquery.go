package helpers

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"io"
	"os"
)

type BQHandle struct {

}
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

func DownloadObjectFromGCS(location string, destConf interface{}, file *os.File) {
	config := destConf.(map[string]interface{})
	if _,ok := config["bucketName"]; !ok{
		panic("bucketName not found")
	}
	bucket := config["bucketName"].(string)
	if _,ok := config["credentials"]; !ok {
		panic("credentials not found")
	}
	credentials := config["credentials"].(string)
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsJSON([]byte(credentials)))
	if err != nil {
		panic(err)
	}
	//gcsBaseUrl := "https://storage.googleapis.com/"
	//location = strings.Replace(location, gcsBaseUrl+bucket+"/", "", 1)
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


func queryBQ(anonymousId string, table string, dataset string, destConfig interface{}) QueryTrackPayload{
	config := destConfig.(map[string]interface{})
	ctx := context.Background()
	if _,ok := config["project"]; !ok {
		panic("project id not found")
	}
	if _,ok := config["credentials"]; !ok {
		panic("credentials not found")
	}
	credentials := config["credentials"].(string)
	projectId := config["project"].(string)
	client, err := bigquery.NewClient(ctx, projectId, option.WithCredentialsJSON([]byte(credentials)))
	if err != nil {
		panic(err)
	}
	q := client.Query(fmt.Sprintf(`select label from %[1]s.%[2]s.%[3]s where anonymous_id = '%[4]s' order by received_at desc limit 1`,projectId, dataset, table, anonymousId ))
	it, err := q.Read(ctx)
	if err != nil {
		panic(err)
	}
	var payload QueryTrackPayload
	for {
		err := it.Next(&payload)
		if err == iterator.Done {
			break
		}
		if err != nil {
			panic(err)
		}

	}
	return payload
}

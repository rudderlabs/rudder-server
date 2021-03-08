package helpers

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	awsS3Manager "github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func DownloadObjectFromS3(location string, destConfig interface{}, file *os.File) {
	config := destConfig.(map[string]interface{})
	if _, ok := config["bucketName"]; !ok {
		panic("bucketName not found")
	}
	if _, ok := config["accessKeyID"]; !ok {
		panic("accessKeyID not found")
	}
	if _, ok := config["accessKey"]; !ok {
		panic("accessKey not found")
	}

	bucket := config["bucketName"].(string)
	accessKeyID := config["accessKeyID"].(string)
	accessKey := config["accessKey"].(string)

	getRegionSession := session.Must(session.NewSession())
	region, err := awsS3Manager.GetBucketRegion(aws.BackgroundContext(), getRegionSession, bucket, "us-east-1")
	if err != nil {
		panic(err)
	}
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(accessKeyID, accessKey, ""),
	}))
	downloader := awsS3Manager.NewDownloader(sess)
	_, err = downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(location),
		})
	if err != nil {
		panic(err)
	}
}

func queryRS(anonymousId string, table string, namespace string, destConfig interface{}) QueryTrackPayload {
	config := destConfig.(map[string]interface{})
	if _, ok := config["user"]; !ok {
		panic("user not found")
	}
	if _, ok := config["password"]; !ok {
		panic("password not found")
	}
	if _, ok := config["host"]; !ok {
		panic("account not found")
	}
	if _, ok := config["database"]; !ok {
		panic("database not found")
	}
	if _, ok := config["port"]; !ok {
		panic("warehouse not found")
	}
	username := config["user"]
	password := config["password"]
	host := config["host"]
	port := config["port"]
	dbName := config["database"]
	url := fmt.Sprintf("sslmode=require user=%v password=%v host=%v port=%v dbname=%v",
		username,
		password,
		host,
		port,
		dbName)
	var err error
	var db *sql.DB
	db, err = sql.Open("postgres", url)
	if err != nil {
		panic(err)
	}
	row := db.QueryRow(fmt.Sprintf(`select label, category, property1, property2, property3, property4, property5 from %s where anonymous_id='%s'`, fmt.Sprintf("%s.%s", namespace, table), anonymousId))
	var label, category, property1, property2, property3, property4, property5 string
	err = row.Scan(&label, &category, &property1, &property2, &property3, &property4, &property5)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	return QueryTrackPayload{
		Label:     label,
		Category:  category,
		Property1: property1,
		Property2: property2,
		Property3: property3,
		Property4: property4,
		Property5: property5,
	}
}

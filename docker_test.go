package main_test

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest"
	main "github.com/rudderlabs/rudder-server"
)

var db *sql.DB
var redisClient *redis.Client
var DB_DSN = "root@tcp(127.0.0.1:3306)/service"
var resource *dockertest.Resource

type Author struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func waitUntilReady(ctx context.Context, endpoint string, atMost, interval time.Duration) {
	probe := time.NewTicker(interval)
	timeout := time.After(atMost)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timeout:
			log.Panicf("application was not ready after %s\n", atMost)
		case <-probe.C:
			resp, err := http.Get(endpoint)
			if err != nil {
				continue
			}
			if resp.StatusCode == http.StatusOK {
				log.Println("application ready")
				return
			}
		}
	}
}

func CreateTablePostgres() {
	_, err := db.Exec("CREATE TABLE example ( id integer, username varchar(255) )")
	if err != nil {
		panic(err)
	}
}

func VerifyHealth() {
	url := fmt.Sprintf("http://localhost:%s/health", "8080")
	method := "GET"

	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		fmt.Println(err)
	}
	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(body))
}
func SendEvent() {

	url := "http://localhost:8080/v1/identify"
	method := "POST"

	payload := strings.NewReader(`{
	"userId": "identified user id",
	"anonymousId":"anon-id-new",
	"context": {
	  "traits": {
		 "trait1": "new-val"  
	  },
	  "ip": "14.5.67.21",
	  "library": {
		  "name": "http"
	  }
	},
	"timestamp": "2020-02-02T00:23:09.544Z"
  }`)

	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		fmt.Println(err)
		return
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Basic MXRmSlZHMlFnNnRoNzdHNjZOZlg4YnRNdFROOg==")

	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(body))
	fmt.Println("Event Sent Successfully")
}

func TestMain(m *testing.M) {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// pulls an redis image, creates a container based on it and runs it
	resourceRedis, err := pool.Run("redis", "alpine3.14", []string{"requirepass=secret"})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		address := fmt.Sprintf("localhost:%s", resourceRedis.GetPort("6379/tcp"))
		redisClient = redis.NewClient(&redis.Options{
			Addr:     address,
			Password: "",
			DB:       0,
		})

		pong, err := redisClient.Ping().Result()
		fmt.Println(pong, err)
		return err
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
	// ----------
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	postgrespool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	database := "jobsdb"
	// pulls an image, creates a container based on it and runs it
	resource, err = postgrespool.Run("postgres", "9.6", []string{"POSTGRES_PASSWORD=password", "POSTGRES_DB=" + database, "POSTGRES_USER=rudder"})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	DB_DSN = fmt.Sprintf("postgres://rudder:password@localhost:%s/%s?sslmode=disable", resource.GetPort("5432/tcp"), database)
	os.Setenv("JOBS_DB_PORT",resource.GetPort("5432/tcp"))
	fmt.Println("************")
	fmt.Println(os.Getenv("JOBS_DB_PORT"))

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := postgrespool.Retry(func() error {
		var err error
		db, err = sql.Open("postgres", DB_DSN)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
	// ----------
	// Set Rudder Transformer
	// pulls an image, creates a container based on it and runs it
	// rudder_trans_pool, err := dockertest.NewPool("")
	// if err != nil {
	// 	log.Fatalf("Could not connect to docker: %s", err)
	// }
	// rudder_tran_res, err := rudder_trans_pool.Run("rudderlabs/rudder-transformer", "latest", []string{"USER=node"})
	// if err != nil {
	// 	log.Fatalf("Could not start resource: %s", err)
	// }
	// waitUntilReady(
	// 	context.Background(),
	// 	fmt.Sprintf("http://localhost:%s/health", rudder_tran_res.GetPort("9090/tcp")),
	// 	time.Minute,
	// 	time.Second,
	// )
	// os.Setenv("JOBS_DB_PORT","7632")

	go main.Run()
	waitUntilReady(
		context.Background(),
		fmt.Sprintf("http://localhost:%s/health", "8080"),
		time.Minute,
		time.Second,
	)
	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	// if err := pool.Purge(resource); err != nil {
	// 	log.Fatalf("Could not purge resource: %s", err)
	// }

	os.Exit(code)
}

func TestSomething(t *testing.T) {
	//Testing postgres Client
	CreateTablePostgres()

	//  Test Rudder docker health point
	VerifyHealth()

	//SEND EVENT
	SendEvent()
	// TODO: Verify in POSTGRES
	// TODO: Verify in Live Evets API
}

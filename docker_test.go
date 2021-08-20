package main_test

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-redis/redis"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest"
	"github.com/phayes/freeport"
	main "github.com/rudderlabs/rudder-server"
)

var (
	hold        bool
	db          *sql.DB
	redisClient *redis.Client
	DB_DSN      = "root@tcp(127.0.0.1:3306)/service"
	httpPort    string
)

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

func blockOnHold() {
	if !hold {
		return
	}

	fmt.Println("Test on hold, before cleanup")
	fmt.Println("Press Ctrl+C to exit")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
	close(c)
}

func CreateTablePostgres() {
	_, err := db.Exec("CREATE TABLE example ( id integer, username varchar(255) )")
	if err != nil {
		panic(err)
	}
}

func VerifyHealth() {
	url := fmt.Sprintf("http://localhost:%s/health", httpPort)
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

	url := fmt.Sprintf("http://localhost:%s/v1/identify", httpPort)
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
	// hack to make defer work, without being affected by the os.Exit in TestMain
	os.Exit(run(m))
}

func run(m *testing.M) int {
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
	defer func() {
		if err := pool.Purge(resourceRedis); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()
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

	database := "jobsdb"
	// pulls an image, creates a container based on it and runs it
	resourcePostgres, err := pool.Run("postgres", "11-alpine", []string{
		"POSTGRES_PASSWORD=password",
		"POSTGRES_DB=" + database,
		"POSTGRES_USER=rudder",
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	defer func() {
		if err := pool.Purge(resourcePostgres); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

	DB_DSN = fmt.Sprintf("postgres://rudder:password@localhost:%s/%s?sslmode=disable", resourcePostgres.GetPort("5432/tcp"), database)
	os.Setenv("JOBS_DB_PORT", resourcePostgres.GetPort("5432/tcp"))
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
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
	transformerRes, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "rudderlabs/rudder-transformer",
		Tag:          "latest",
		ExposedPorts: []string{"9090"},
		Env: []string{
			"CONFIG_BACKEND_URL=https://api.dev.rudderlabs.com",
		},
	})
	defer func() {
		if err := pool.Purge(transformerRes); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

	transformURL := fmt.Sprintf("http://localhost:%s", transformerRes.GetPort("9090/tcp"))
	waitUntilReady(
		context.Background(),
		fmt.Sprintf("%s/health", transformURL),
		time.Minute,
		time.Second,
	)
	os.Setenv("DEST_TRANSFORM_URL", transformURL)

	httpPortInt, err := freeport.GetFreePort()
	if err != nil {
		log.Panic(err)
	}
	httpPort = strconv.Itoa(httpPortInt)
	os.Setenv("RSERVER_GATEWAY_WEB_PORT", httpPort)
	httpAdminPort, err := freeport.GetFreePort()
	if err != nil {
		log.Panic(err)
	}
	os.Setenv("RSERVER_GATEWAY_ADMIN_WEB_PORT", strconv.Itoa(httpAdminPort))

	os.Setenv("RSERVER_ENABLE_STATS", "false")

	svcCtx, svcCancel := context.WithCancel(context.Background())
	go main.Run(svcCtx)

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%s/health", httpPort)
	fmt.Println("serviceHealthEndpoint", serviceHealthEndpoint)
	waitUntilReady(
		context.Background(),
		serviceHealthEndpoint,
		time.Minute,
		time.Second,
	)
	code := m.Run()
	blockOnHold()

	svcCancel()
	fmt.Println("test done, ignore errors bellow:")

	// wait for the service to be stopped
	pool.Retry(func() error {
		_, err := http.Get(serviceHealthEndpoint)
		if err != nil {
			return nil
		}
		return fmt.Errorf("still working")
	})

	return code
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

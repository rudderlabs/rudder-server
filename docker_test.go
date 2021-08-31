package main_test

import (
	"encoding/json"
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
	"github.com/rudderlabs/rudder-server/jobsdb"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	hold        bool=true
	db          *sql.DB
	redisClient *redis.Client
	DB_DSN      = "root@tcp(127.0.0.1:3306)/service"
	httpPort    string
	dbHandle *sql.DB
	sourceJSON backendconfig.ConfigT
	webhookurl string
)

type WebHook struct {
	Data []struct {
		UUID      string      `json:"uuid"`
		Type      string      `json:"type"`
		TokenID   string      `json:"token_id"`
		IP        string      `json:"ip"`
		Hostname  string      `json:"hostname"`
		Method    string      `json:"method"`
		UserAgent string      `json:"user_agent"`
		Content   string   `json:"content"`
		Query     interface{} `json:"query"`
		Headers   struct {
			Connection     []string `json:"connection"`
			AcceptEncoding []string `json:"accept-encoding"`
			ContentType    []string `json:"content-type"`
			ContentLength  []string `json:"content-length"`
			UserAgent      []string `json:"user-agent"`
			Host           []string `json:"host"`
		} `json:"headers"`
		URL                string        `json:"url"`
		Size               int           `json:"size"`
		Files              []interface{} `json:"files"`
		CreatedAt          string        `json:"created_at"`
		UpdatedAt          string        `json:"updated_at"`
		Sorting            int64         `json:"sorting"`
		CustomActionOutput []interface{} `json:"custom_action_output"`
	} `json:"data"`
	Total       int  `json:"total"`
	PerPage     int  `json:"per_page"`
	CurrentPage int  `json:"current_page"`
	IsLastPage  bool `json:"is_last_page"`
	From        int  `json:"from"`
	To          int  `json:"to"`
}

type Author struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func getWorkspaceConfig() backendconfig.ConfigT {
	backendConfig := new(backendconfig.WorkspaceConfig)
	sourceJSON, _ := backendConfig.Get()
	return sourceJSON
}

func initializeWarehouseConfig(src string, des string) map[string][]warehouseutils.WarehouseT {
	var warehouses = make(map[string][]warehouseutils.WarehouseT)
	for _, source := range sourceJSON.Sources {
		if source.Name == src {
			if len(source.Destinations) > 0 {
				for _, destination := range source.Destinations {
					  if destination.Name == des{
						warehouses[destination.DestinationDefinition.Name] = append(warehouses[destination.DestinationDefinition.Name], 
						warehouseutils.WarehouseT{Source: source, Destination: destination})
						return warehouses
				}}
			}
		}
	}
	return warehouses
}

// getFromFile reads the workspace config from JSON file
func getWebhookResponse() (int, bool){
    filePath := "./webhooktest.json";
    fmt.Printf( "// reading file %s\n", filePath )
    file, err1 := ioutil.ReadFile( filePath )
    if err1 != nil {
        fmt.Printf( "// error while reading file %s\n", filePath )
        fmt.Printf("File error: %v\n", err1)
        os.Exit(1)
    }
    var WebHookstruct WebHook

    err2 := json.Unmarshal(file, &WebHookstruct)
    if err2 != nil {
        fmt.Println("error:", err2)
        os.Exit(1)
    }
	// fmt.Printf("%+v\n", WebHookstruct)
    return WebHookstruct.Total, true
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

func GetDestinationWebhookEvent() string {
	
	url := fmt.Sprintf("%s/requests?page=1&password=&sorting=oldest", webhookurl)
    url = strings.Replace(url, "https://webhook.site", "https://webhook.site/token", -1)
	fmt.Println("GetDestinationWebhookEvent:- ",url)
	method := "GET"

	client := &http.Client {
	}
	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		fmt.Println(err)
		return ""
	}
	req.Header.Add("Cookie", "laravel_session=UZyYPg2FfrV0UNnxsuKKKYEOzGVScROClyvjWYmx")

	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return ""
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return ""
	}
	_ = ioutil.WriteFile("webhooktest.json", body, 0644)
	return string(body)
}

func SendEvent() {
	fmt.Println("Sending Track Event")
	url := fmt.Sprintf("http://localhost:%s/v1/track", httpPort)
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
  
	client := &http.Client {
	}
	req, err := http.NewRequest(method, url, payload)
  
	if err != nil {
	  fmt.Println(err)
	  return
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Basic MXhJY0tneXp5QjFyNnV0S0F0TzZlamg4b0tJOg==")
  
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
	os.Setenv("WAREHOUSE_JOBS_DB_PORT",resourcePostgres.GetPort("5432/tcp"))
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

	os.Setenv("RUDDER_ADMIN_PASSWORD", "password")

	os.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_FROM_FILE", "true")

	mydir, _ := os.Getwd()
	CONFIG_JSONPATH := mydir + "/workspaceConfig.json"
	os.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", CONFIG_JSONPATH)

	os.Setenv("WORKSPACE_TOKEN", "1vLbwltztKUgpuFxmJlSe1esX8c")

	os.Setenv("CONFIG_BACKEND_URL", "https://api.dev.rudderlabs.com")

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

func TestWebhook(t *testing.T) {
	//Testing postgres Client
	CreateTablePostgres()

	//  Test Rudder docker health point
	VerifyHealth()

	// 
	var err error
	psqlInfo := jobsdb.GetConnectionString()
	dbHandle, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	// Pulling config form workspaceConfig.json
	sourceJSON = getWorkspaceConfig()

	warehouses :=  initializeWarehouseConfig("Dev Integration Test 1", "Des WebHook Integration Test 1")
	webhookurl = fmt.Sprintf("%+v", warehouses["WEBHOOK"][0].Destination.Config["webhookUrl"])
	fmt.Printf("%+v\n", webhookurl)
 	GetDestinationWebhookEvent()
	var beforeWebHookResponseTotal int
	beforeWebHookResponseTotal, _ = getWebhookResponse()
	fmt.Println("beforeWebHookResponseTotal := ", beforeWebHookResponseTotal)
	
	//SEND EVENT
	SendEvent()
	time.Sleep(300 * time.Second)
	GetDestinationWebhookEvent()
	var afterWebHookResponseTotal int
	afterWebHookResponseTotal, _ = getWebhookResponse()
	fmt.Println("afterWebHookResponseTotal := ", afterWebHookResponseTotal)

	if afterWebHookResponseTotal == beforeWebHookResponseTotal+1{
		fmt.Println("TC01 PASSED")
		} else {
		fmt.Println("TC01 FAILED")
		os.Exit(1)
		}

	// TODO: Verify in POSTGRES
	// TODO: Verify in Live Evets API
}

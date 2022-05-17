package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	uuid "github.com/gofrs/uuid"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"github.com/rs/cors"
)

var (
	host, user, password, dbname, sslmode, configBackendURL string
	port                                                    int
	globalDBHandle                                          *sql.DB
	multitenantSecret                                       string
	configString                                            string
	writeKeyToWorkspaceIDMap                                map[string]string
	dbWriteLock                                             sync.RWMutex
	job_id                                                  int64
	isInitialized                                           bool
	cbeInitialised                                          chan string
)

type DestinationT struct {
	ID      string `json:"id,omitempty"`
	Enabled bool   `json:"enabled,omitempty"`
	Deleted bool   `json:"deleted,omitempty"`
}

type SourceT struct {
	ID           string         `json:"id,omitempty"`
	Enabled      bool           `json:"enabled,omitempty"`
	Deleted      bool           `json:"deleted,omitempty"`
	WorkspaceID  string         `json:"workspaceId,omitempty"`
	Destinations []DestinationT `json:"destinations,omitempty"`
	WriteKey     string         `json:"writeKey,omitempty"`
}

type WorkspacesStruct struct {
	Ack_key    string `json:"ack_key,omitempty"`
	Workspaces string `json:"workspaces,omitempty"`
}

type ServerModeStruct struct {
	Status_key string `json:"ack_key,omitempty"`
	Mode       string `json:"mode,omitempty"`
}

func init() {
	host = GetEnv("JOBS_DB_HOST", "localhost")
	user = GetEnv("JOBS_DB_USER", "rudder")
	dbname = GetEnv("JOBS_DB_DB_NAME", "jobsdb")
	port, _ = strconv.Atoi(GetEnv("JOBS_DB_PORT", "6432"))
	password = GetEnv("JOBS_DB_PASSWORD", "password") // Reading secrets from
	sslmode = GetEnv("JOBS_DB_SSL_MODE", "disable")
	configBackendURL = GetEnv("CONFIG_BACKEND_URL", "http://api.dev.rudderlabs.com")
	multitenantSecret = GetEnv("MULTITENANT_SECRET", "xL%A=8fw3W")
}

func main() {
	setupDB()
	cbeInitialised = make(chan string)
	go setupConfigBackendData()
	cli := setupETCDClient()
	go clusterSequences(cli)
	srvMux := mux.NewRouter()
	srvMux.HandleFunc("/v1/batch", mockGatewayHandler).Methods("POST")
	srvMux.HandleFunc("/health", mockHealthHandler).Methods("GET")

	c := cors.New(cors.Options{
		AllowOriginFunc:  reflectOrigin,
		AllowCredentials: true,
		AllowedHeaders:   []string{"*"},
		MaxAge:           900, // 15 mins
	})

	httpWebServer := &http.Server{
		Addr:              ":" + strconv.Itoa(8080),
		Handler:           c.Handler(srvMux),
		ReadTimeout:       0 * time.Second,
		ReadHeaderTimeout: 0 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       720 * time.Second,
		MaxHeaderBytes:    524288,
	}
	<-cbeInitialised
	httpWebServer.ListenAndServe()
}

func setupETCDClient() *clientv3.Client {
	etcdHosts := strings.Split(GetEnv("ETCD_HOSTS", "127.0.0.1:2379"), `,`)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:            etcdHosts,
		DialTimeout:          20 * time.Second,
		DialKeepAliveTime:    30 * time.Second,
		DialKeepAliveTimeout: 10 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithBlock(), // block until the underlying connection is up
		},
	})
	if err != nil {
		panic(err)
	}
	return cli
}
func clusterSequences(cli *clientv3.Client) {
	var result string
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	instance_id := strings.Split(GetEnv("INSTANCE_ID", "-1"), "-")[3]
	newWorkspaceGWKey := "/" + GetEnv("RELEASE_NAME", `multitenantv1`) + "/SERVER/" + instance_id + "/gateway/WORKSPACES"
	newWorkspaceGWKVS, err := cli.Get(ctx, newWorkspaceGWKey)
	newWorkspaceGWVersion := int64(0)
	if err != nil {
		panic(err)
	}
	if len(newWorkspaceGWKVS.Kvs) > 0 {
		result = string(newWorkspaceGWKVS.Kvs[0].Value)
		newWorkspaceGWVersion = newWorkspaceGWKVS.Header.Revision
	} else {
		result = ``
	}
	var workspaceGWKey WorkspacesStruct
	if result != "" {
		err = json.Unmarshal([]byte(result), &workspaceGWKey)
		if err != nil {
			panic(err)
		}
		_, err = cli.Put(ctx, workspaceGWKey.Ack_key, `{
            "status": "RELOADED"
        }`)
		if err != nil {
			panic(err)
		}
	}
	newWorkspaceProcKey := "/" + GetEnv("RELEASE_NAME", `multitenantv1`) + "/SERVER/" + instance_id + "/processor/WORKSPACES"
	newWorkspaceProcKVS, err := cli.Get(ctx, newWorkspaceProcKey)
	newWorkspaceProcVersion := int64(0)
	if err != nil {
		panic(err)
	}
	if len(newWorkspaceProcKVS.Kvs) > 0 {
		result = string(newWorkspaceProcKVS.Kvs[0].Value)
		newWorkspaceProcVersion = newWorkspaceProcKVS.Header.Revision
	} else {
		result = ``
	}
	var workspaceProcKey WorkspacesStruct
	if result != "" {
		err = json.Unmarshal([]byte(result), &workspaceProcKey)
		if err != nil {
			panic(err)
		}
		_, err = cli.Put(ctx, workspaceProcKey.Ack_key, `{
            "status": "RELOADED"
        }`)
		if err != nil {
			panic(err)
		}
	}
	serverModeKey := "/" + GetEnv("RELEASE_NAME", `multitenantv1`) + "/SERVER/" + instance_id + "/MODE"
	serverModeKVS, err := cli.Get(ctx, serverModeKey)
	serverModeVersion := int64(0)
	if err != nil {
		panic(err)
	}
	if len(serverModeKVS.Kvs) > 0 {
		result = string(serverModeKVS.Kvs[0].Value)
		serverModeVersion = serverModeKVS.Header.Revision
	} else {
		result = ``
	}
	var serverModeKeyStruct ServerModeStruct
	if result != "" {
		err = json.Unmarshal([]byte(result), &serverModeKeyStruct)
		if err != nil {
			panic(err)
		}
		_, err = cli.Put(ctx, serverModeKeyStruct.Status_key, fmt.Sprintf(`{"status": %v}`, serverModeKeyStruct.Mode))
		if err != nil {
			panic(err)
		}
	}
	workspacesWatchGWChan := WatchForKey(cli, ctx, newWorkspaceGWKey, newWorkspaceGWVersion)
	workspacesWatchProcChan := WatchForKey(cli, ctx, newWorkspaceProcKey, newWorkspaceProcVersion)
	serverModeKeyChan := WatchForKey(cli, ctx, serverModeKey, serverModeVersion)
	for {
		select {
		case val := <-workspacesWatchGWChan:
			var workspaceKey WorkspacesStruct
			log.Println("Inside Workspaes For Loop (GW)", val)
			if val != "" {
				err = json.Unmarshal([]byte(val), &workspaceKey)
				if err != nil {
					panic(err)
				}
				_, err = cli.Put(ctx, workspaceKey.Ack_key, `{"status": "RELOADED"}`)
				if err != nil {
					panic(err)
				}
			}
		case val := <-workspacesWatchProcChan:
			var workspaceKey WorkspacesStruct
			log.Println("Inside Workspaes For Loop (Proc)", val)
			if val != "" {
				err = json.Unmarshal([]byte(val), &workspaceKey)
				if err != nil {
					panic(err)
				}
				_, err = cli.Put(ctx, workspaceKey.Ack_key, `{"status": "RELOADED"}`)
				if err != nil {
					panic(err)
				}
			}
		case val := <-serverModeKeyChan:
			var serverModeKeyStruct ServerModeStruct
			if val != "" {
				err = json.Unmarshal([]byte(val), &serverModeKeyStruct)
				if err != nil {
					panic(err)
				}
				_, err = cli.Put(ctx, serverModeKeyStruct.Status_key, fmt.Sprintf(`{"status": %v}`, serverModeKeyStruct.Mode))
				if err != nil {
					panic(err)
				}
			}
		default:
			log.Println("Inside Default Sleeping for 10s")
			time.Sleep(10 * time.Second)
		}
	}
}

func WatchForKey(cli *clientv3.Client, ctx context.Context, key string, version int64) chan string {
	resultChan := make(chan string)
	go func(returnChan chan string, ctx context.Context, key string) {
		opts := []clientv3.OpOption{clientv3.WithRev(version), clientv3.WithPrefix()}
		etcdWatchChan := cli.Watch(ctx, key, opts...)
		for watchResp := range etcdWatchChan {
			for _, event := range watchResp.Events {
				switch event.Type {
				case mvccpb.PUT:
					log.Println("PUT:", string(event.Kv.Value))
					returnChan <- string(event.Kv.Value)
				}
			}
		}
		// close(resultChan)
	}(resultChan, ctx, key)
	return resultChan
}

func setupDB() {
	var err error
	psqlInfo := GetConnectionString()
	globalDBHandle, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	createDS()
	var maxID sql.NullInt64
	sqlStatement := fmt.Sprintf(`SELECT MAX(job_id) FROM "%s"`, "gw_jobs_1")
	rows, err := globalDBHandle.Query(sqlStatement)
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		err := rows.Scan(&maxID)
		if err != nil {
			panic(err)
		}
		break
	}
	if maxID.Valid {
		job_id = int64(maxID.Int64)
	}
	rows.Close()
}

func setupConfigBackendData() {
	writeKeyToWorkspaceIDMap = make(map[string]string)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		req, err := http.NewRequestWithContext(ctx, "GET", configBackendURL+"/allWorkspaceConfigUpdates", nil)
		defer cancel()
		if err != nil {
			panic(err)
		}
		req.SetBasicAuth(multitenantSecret, "")
		req.Header.Set("Content-Type", "application/json")
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		var respBody []byte
		if resp != nil && resp.Body != nil {
			respBody, _ = io.ReadAll(resp.Body)
			defer resp.Body.Close()
		}

		var workspaces map[string][]SourceT
		err = json.Unmarshal(respBody, &workspaces)
		if err != nil {
			panic(err)
		}
		for workspaceID, Sources := range workspaces {
			for _, source := range Sources {
				writeKeyToWorkspaceIDMap[source.WriteKey] = workspaceID
			}
		}
		log.Println("Before isInitialised")
		if !isInitialized {
			log.Println("Inside isInitialised True")
			isInitialized = true
			close(cbeInitialised)
		}
		time.Sleep(100 * time.Second)
	}
}

func GetConnectionString() string {
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=%s",
		host, port, user, password, dbname, sslmode)
}

func createDS() {
	jobsTables := []string{"gw_jobs_1", "rt_jobs_1", "batch_rt_jobs_1", "proc_error_jobs_1"}
	jobStatusTables := []string{"gw_job_status_1", "rt_job_status_1", "batch_rt_job_status_1", "proc_error_job_status_1"}

	for i, jobTable := range jobsTables {
		//Create the jobs and job_status tables
		sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (
                job_id BIGSERIAL PRIMARY KEY,
                workspace_id TEXT NOT NULL DEFAULT '',
                uuid UUID NOT NULL,
                user_id TEXT NOT NULL,
                parameters JSONB NOT NULL,
                custom_val VARCHAR(64) NOT NULL,
                event_payload JSONB NOT NULL,
                event_count INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                expire_at TIMESTAMP NOT NULL DEFAULT NOW());`, jobTable)

		_, err := globalDBHandle.Exec(sqlStatement)
		if err != nil {
			panic(err)
		}

		sqlStatement = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (
            id BIGSERIAL,
            job_id BIGINT REFERENCES "%s"(job_id),
            job_state VARCHAR(64),
            attempt SMALLINT,
            exec_time TIMESTAMP,
            retry_time TIMESTAMP,
            error_code VARCHAR(32),
            error_response JSONB DEFAULT '{}'::JSONB,
            parameters JSONB DEFAULT '{}'::JSONB,
            PRIMARY KEY (job_id, job_state, id));`, jobStatusTables[i], jobTable)
		_, err = globalDBHandle.Exec(sqlStatement)
		if err != nil {
			panic(err)
		}
	}
}

func Store(uuid, user_id, custom_val, workspace_id string, parameters, event_payload json.RawMessage, job_id int64) {
	sqlStatement := fmt.Sprintf(`INSERT INTO "%s" (uuid, user_id, custom_val, parameters, event_payload, workspace_id , job_id)
    VALUES ($1, $2, $3, $4, (regexp_replace($5::text, '\\u0000', '', 'g'))::json , $6 , $7) RETURNING job_id`, "gw_jobs_1")
	stmt, err := globalDBHandle.Prepare(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()
	_, err = stmt.Exec(uuid, user_id, custom_val, string(parameters), string(event_payload), workspace_id, job_id)
	if err != nil {
		panic(err)
	}
}

func GetEnv(key string, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

func reflectOrigin(origin string) bool {
	return true
}

func mockHealthHandler(w http.ResponseWriter, r *http.Request) {
	healthVal := fmt.Sprintf(`{"appType": "%s", "server":"UP", "db":"%s","acceptingEvents":"TRUE","routingEvents":"%s","mode":"%s","goroutines":"%d", "backendConfigMode": "%s", "lastSync":"%s", "lastRegulationSync":"%s"}`, "EMBEDDED", "UP", "true", "UP", 1234, "UP", "UP", "UP")
	_, _ = w.Write([]byte(healthVal))
}

func mockGatewayHandler(w http.ResponseWriter, r *http.Request) {
	payload, writeKey, err := getPayloadAndWriteKey(w, r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	workspaceID := writeKeyToWorkspaceIDMap[writeKey]
	log.Println(workspaceID)
	uid := uuid.Must(uuid.NewV4()).String()
	dbWriteLock.Lock()
	job_id = job_id + 1
	Store(uid, uid, "GW", workspaceID, []byte(`"{key:value}"`), payload, job_id)
	dbWriteLock.Unlock()

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

func getPayloadAndWriteKey(w http.ResponseWriter, r *http.Request) ([]byte, string, error) {
	var err error
	writeKey, _, ok := r.BasicAuth()
	if !ok || writeKey == "" {
		return []byte{}, "", err
	}
	payload, err := getPayloadFromRequest(r)
	if err != nil {
		return []byte{}, writeKey, fmt.Errorf("read payload from request: %w", err)
	}
	return payload, writeKey, err
}

func getPayloadFromRequest(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return []byte{}, errors.New("response.RequestBodyNil")
	}

	payload, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		return payload, fmt.Errorf("read all request body: %w", err)
	}
	return payload, nil
}

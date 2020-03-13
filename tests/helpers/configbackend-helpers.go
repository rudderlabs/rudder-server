package helpers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/rudderlabs/rudder-server/config"
)

var (
	host, user, password, dbname string
	configBackendURL             string
	configBackendAdminUser       string
	configBackendAdminPassword   string
	port                         int
)

func init() {
	loadConfig()
}

// Loads db config from config file
func loadConfig() {
	host = config.GetEnv("CONFIG_DB_HOST", "localhost")
	user = config.GetEnv("CONFIG_DB_USER", "postgres")
	dbname = config.GetEnv("CONFIG_DB_DB_NAME", "postgresDB")
	port, _ = strconv.Atoi(config.GetEnv("CONFIG_DB_PORT", "5433"))
	password = config.GetEnv("CONFIG_DB_PASSWORD", "postgres")
	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
	configBackendAdminUser = config.GetEnv("CONFIG_BACKEND_ADMIN_USER", "")
	configBackendAdminPassword = config.GetEnv("CONFIG_BACKEND_ADMIN_PASSWORD", "")
}

func GetConnectionString() string {
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
}

// ToggleEventUpload toggles event upload flag for a source
func ToggleEventUpload(sourceID string, uploadEvents bool) int {
	obj := map[string]bool{
		"eventUpload": uploadEvents,
	}
	var toggleEventPostObject, _ = json.Marshal(obj)
	toggleEventUploadURL := configBackendURL + "/admin/sources/" + sourceID + "/toggleEventUpload"
	req, err := http.NewRequest("POST", toggleEventUploadURL, bytes.NewBuffer(toggleEventPostObject))
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(configBackendAdminUser, configBackendAdminPassword)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// FetchEventSchemaCount returns count of event_uploads table
func FetchEventSchemaCount(sourceID string) int {
	fetchEventsURL := configBackendURL + "/admin/sources/" + sourceID + "/eventUploads"

	req, err := http.NewRequest("GET", fetchEventsURL, nil)
	req.SetBasicAuth(configBackendAdminUser, configBackendAdminPassword)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	var events []interface{}
	json.Unmarshal(body, &events)
	return len(events)
}

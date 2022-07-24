package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"text/template"
)

var (
	snowflakeIntegrationTestCredentials = "SNOWFLAKE_INTEGRATION_TEST_USER_CRED"
	redshiftIntegrationTestCredentials  = "REDSHIFT_INTEGRATION_TEST_USER_CRED"
	deltalakeIntegrationTestCredentials = "DATABRICKS_INTEGRATION_TEST_USER_CRED"
	workspaceConfigPath                 = "/etc/rudderstack/workspaceConfig.json"
)

func main() {
	log.Println("Started populating workspace config for warehouse integration tests")

	credentials, err := populateCredentials()
	if err != nil {
		log.Panicf("Error occurred while populating credentilas with error: %s", err.Error())
	}

	t, err := template.ParseFiles("warehouse/testdata/workspaceConfig/template.json")
	if err != nil {
		log.Panicf("Error occurred while parsing files for template path with error: %s", err.Error())
	}

	err = os.MkdirAll(filepath.Dir(workspaceConfigPath), os.ModePerm)
	if err != nil {
		log.Panicf("Error occurred while making directory paths error: %s", err.Error())
	}

	f, err := os.OpenFile(workspaceConfigPath, os.O_RDWR|os.O_CREATE, 0o755)
	if err != nil {
		log.Panicf("Error occurred while creating workspaceConfig.json file with error: %s", err.Error())
	}
	defer func() { _ = f.Close() }()

	err = t.Execute(f, credentials)
	if err != nil {
		log.Panicf("Error occurred while executing template path files with error: %s", err.Error())
	}

	log.Println("Completed populating workspace config for warehouse integration tests")
}

func populateCredentials() (values map[string]string, err error) {
	values = make(map[string]string)

	var credentials map[string]string
	if credentials, err = credentialsFromKey(snowflakeIntegrationTestCredentials); err != nil {
		return
	}
	for k, v := range credentials {
		values[fmt.Sprintf("sf_%s", k)] = v
	}

	if credentials, err = credentialsFromKey(redshiftIntegrationTestCredentials); err != nil {
		return
	}
	for k, v := range credentials {
		values[fmt.Sprintf("rs_%s", k)] = v
	}

	if credentials, err = credentialsFromKey(deltalakeIntegrationTestCredentials); err != nil {
		return
	}
	for k, v := range credentials {
		values[fmt.Sprintf("dl_%s", k)] = v
	}
	return
}

func credentialsFromKey(key string) (credentials map[string]string, err error) {
	cred, exists := os.LookupEnv(key)
	if !exists {
		err = fmt.Errorf("following %s does not exists while setting up the workspace config", key)
		return
	}

	err = json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		err = fmt.Errorf("error occurred while unmarshalling test credentials for key %s with err: %s", key, err.Error())
		return
	}
	return
}

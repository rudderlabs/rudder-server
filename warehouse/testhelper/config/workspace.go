package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"text/template"
)

func main() {
	createWorkspaceConfig()
}

func credentialsFromEnv(env string) (credentials map[string]string) {
	cred := os.Getenv(env)
	if cred == "" {
		log.Panicf("Error occurred while getting env variable %s", env)
	}

	var err error
	err = json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		log.Panicf("Error occurred while unmarshalling env variable %s test credentials with error: %s", env, err.Error())
	}
	return
}

func createWorkspaceConfig() {
	values := map[string]string{}

	log.Println("Populating snowflake credentials")
	sfCredentials := credentialsFromEnv("SNOWFLAKE_INTEGRATION_TEST_USER_CRED")
	for k, v := range sfCredentials {
		values[fmt.Sprintf("sf_%s", k)] = v
	}

	log.Println("Populating redshift credentials")
	rsCredentials := credentialsFromEnv("REDSHIFT_INTEGRATION_TEST_USER_CRED")
	for k, v := range rsCredentials {
		values[fmt.Sprintf("rs_%s", k)] = v
	}

	log.Println("Populating deltalake credentials")
	dlCredentials := credentialsFromEnv("DATABRICKS_INTEGRATION_TEST_USER_CRED")
	for k, v := range dlCredentials {
		values[fmt.Sprintf("dl_%s", k)] = v
	}

	t, err := template.ParseFiles("warehouse/testdata/workspaceConfig/template.json")
	if err != nil {
		log.Panicf("Error occurred while parsing files for template path with error: %s", err.Error())
	}

	f, err := os.Create("warehouse/testdata/workspaceConfig/config.json")
	if err != nil {
		log.Panicf("Error occurred while creating temp path with error: %s", err.Error())
	}
	defer func() { _ = f.Close() }()

	err = t.Execute(f, values)
	if err != nil {
		log.Panicf("Error occurred while executing template path files with error: %s", err.Error())
	}

	err = os.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", f.Name())
	if err != nil {
		log.Panicf("Error occurred while setting env for config json path with error: %s", err.Error())
	}
}

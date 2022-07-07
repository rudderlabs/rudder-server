package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"text/template"
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

	f, err := os.Create("warehouse/testdata/workspaceConfig/config.json")
	if err != nil {
		log.Panicf("Error occurred while creating temp path with error: %s", err.Error())
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

	//log.Println("Populating snowflake credentials")
	//sfCredentials, err := credentialsFromEnv(testhelper.SnowflakeIntegrationTestUserCred)
	//if err != nil {
	//	return
	//}
	//for k, v := range sfCredentials {
	//	values[fmt.Sprintf("sf_%s", k)] = v
	//}
	//
	//log.Println("Populating redshift credentials")
	//rsCredentials, err := credentialsFromEnv(testhelper.RedshiftIntegrationTestUserCred)
	//if err != nil {
	//	return
	//}
	//for k, v := range rsCredentials {
	//	values[fmt.Sprintf("rs_%s", k)] = v
	//}
	//
	//log.Println("Populating deltalake credentials")
	//dlCredentials, err := credentialsFromEnv(testhelper.DatabricksIntegrationTestUserCred)
	//if err != nil {
	//	return
	//}
	//for k, v := range dlCredentials {
	//	values[fmt.Sprintf("dl_%s", k)] = v
	//}
	return
}

func credentialsFromEnv(env string) (credentials map[string]string, err error) {
	cred := os.Getenv(env)
	if cred == "" {
		err = fmt.Errorf("error occurred while getting env variable %s", env)
		return
	}

	err = json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		err = fmt.Errorf("error occurred while unmarshalling env variable %s test credentials with error: %w", env, err)
		return
	}
	return
}

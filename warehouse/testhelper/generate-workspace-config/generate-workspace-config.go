package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
)

func main() {
	log.Println("Started populating workspace config")

	t, err := template.ParseFiles("warehouse/testdata/workspaceConfig/template.json")
	if err != nil {
		log.Panicf("Error occurred while parsing files for template path with error: %s", err.Error())
	}

	err = os.MkdirAll(filepath.Dir(testhelper.WorkspaceConfigPath), os.ModePerm)
	if err != nil {
		log.Panicf("Error occurred while making directory paths error: %s", err.Error())
	}

	f, err := os.OpenFile(testhelper.WorkspaceConfigPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		log.Panicf("Error occurred while creating workspaceConfig.json file with error: %s", err.Error())
	}
	defer func() { _ = f.Close() }()

	templateConfigurations := populateTemplateConfigurations()
	err = t.Execute(f, templateConfigurations)
	if err != nil {
		log.Panicf("Error occurred while executing template path files with error: %s", err.Error())
	}

	log.Println("Completed populating workspace config for warehouse integration tests")
}

func populateTemplateConfigurations() map[string]string {
	values := map[string]string{
		"workspaceId": "BpLnfgDsc2WD8F2qNfHK5a84jjJ",

		"postgresWriteKey": "kwzDkh9h2fhfUVuS9jZ8uVbhV3v",
		"postgresHost":     "wh-postgres",
		"postgresDatabase": "rudderdb",
		"postgresUser":     "rudder",
		"postgresPassword": "rudder-password",
		"postgresPort":     "5432",

		"clickHouseWriteKey": "C5AWX39IVUWSP2NcHciWvqZTa2N",
		"clickHouseHost":     "wh-clickhouse",
		"clickHouseDatabase": "rudderdb",
		"clickHouseUser":     "rudder",
		"clickHousePassword": "rudder-password",
		"clickHousePort":     "9000",

		"clickhouseClusterWriteKey": "95RxRTZHWUsaD6HEdz0ThbXfQ6p",
		"clickhouseClusterHost":     "wh-clickhouse01",
		"clickhouseClusterDatabase": "rudderdb",
		"clickhouseClusterCluster":  "rudder_cluster",
		"clickhouseClusterUser":     "rudder",
		"clickhouseClusterPassword": "rudder-password",
		"clickhouseClusterPort":     "9000",

		"mssqlWriteKey": "YSQ3n267l1VQKGNbSuJE9fQbzON",
		"mssqlHost":     "wh-mssql",
		"mssqlDatabase": "master",
		"mssqlUser":     "SA",
		"mssqlPassword": "reallyStrongPwd123",
		"mssqlPort":     "1433",

		"bigqueryWriteKey":  "J77aX7tLFJ84qYU6UrN8ctecwZt",
		"snowflakeWriteKey": "2eSJyYtqwcFiUILzXv2fcNIrWO7",
		"redshiftWriteKey":  "JAAwdCxmM8BIabKERsUhPNmMmdf",
		"deltalakeWriteKey": "sToFgoilA0U1WxNeW1gdgUVDsEW",

		"minioBucketName":      "devintegrationtest",
		"minioAccesskeyID":     "MYACCESSKEY",
		"minioSecretAccessKey": "MYSECRETKEY",
		"minioEndpoint":        "wh-minio:9000",
	}

	for k, v := range credentialsFromKey(testhelper.SnowflakeIntegrationTestCredentials) {
		values[fmt.Sprintf("snowflake%s", k)] = v
	}
	for k, v := range credentialsFromKey(testhelper.RedshiftIntegrationTestCredentials) {
		values[fmt.Sprintf("redshift%s", k)] = v
	}
	for k, v := range credentialsFromKey(testhelper.DeltalakeIntegrationTestCredentials) {
		values[fmt.Sprintf("deltalake%s", k)] = v
	}
	for k, v := range credentialsFromKey(testhelper.BigqueryIntegrationTestCredentials) {
		values[fmt.Sprintf("bigquery%s", k)] = v
	}
	enhanceBQCredentials(values)
	enhanceNamespace(values)
	return values
}

func enhanceNamespace(values map[string]string) {
	values["snowflakeNamespace"] = testhelper.GetSchema(warehouseutils.SNOWFLAKE, testhelper.SnowflakeIntegrationTestSchema)
	values["redshiftNamespace"] = testhelper.GetSchema(warehouseutils.RS, testhelper.RedshiftIntegrationTestSchema)
	values["bigqueryNamespace"] = testhelper.GetSchema(warehouseutils.BQ, testhelper.BigqueryIntegrationTestSchema)
	values["deltalakeNamespace"] = testhelper.GetSchema(warehouseutils.DELTALAKE, testhelper.DeltalakeIntegrationTestSchema)
}

func enhanceBQCredentials(values map[string]string) {
	key := "bigqueryCredentials"
	if credentials, exists := values[key]; exists {
		escapedCredentials, err := json.Marshal(credentials)
		if err != nil {
			log.Panicf("error escaping big query JSON credentials while setting up the workspace config with error: %s", err.Error())
		}
		values[key] = strings.Trim(string(escapedCredentials), `"`)
	}
}

func credentialsFromKey(key string) (credentials map[string]string) {
	cred, exists := os.LookupEnv(key)
	if !exists {
		log.Print(fmt.Errorf("env %s does not exists while setting up the workspace config", key))
		return
	}

	err := json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		log.Panicf("error occurred while unmarshalling %s for setting up the workspace config", key)
		return
	}
	return
}

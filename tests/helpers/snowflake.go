package helpers

import (
	"database/sql"
	"fmt"
	_ "github.com/snowflakedb/gosnowflake"
	"strings"
)

func querySnowflake(anonymousId string, table string, schema string, destConfig interface{}) QueryTrackPayload{
	config := destConfig.(map[string]interface{})
	fmt.Println(config)
	if _,ok:= config["user"]; !ok {
		panic("user not found")
	}
	if _,ok:= config["password"]; !ok {
		panic("password not found")
	}
	if _,ok:= config["account"]; !ok {
		panic("account not found")
	}
	if _,ok:= config["database"]; !ok {
		panic("database not found")
	}
	if _,ok:= config["warehouse"]; !ok {
		panic("warehouse not found")
	}
	username:=config["user"]
	password:=config["password"]
	account:=config["account"]
	database:=config["database"]
	warehouse:=config["warehouse"]

	url := fmt.Sprintf("%s:%s@%s/%s?warehouse=%s",
		username,
		password,
		account,
		database,
		warehouse)

	if schema != "" {
		url += fmt.Sprintf("&schema=%s", schema)
	}
	db, err := sql.Open("snowflake", url)
	if err!=nil {
		panic(err)
	}
	row:=db.QueryRow(fmt.Sprintf(`select label from %s where ANONYMOUS_ID='%s'`,strings.ToUpper(table), anonymousId))
	var label string
	err = row.Scan(&label)
	fmt.Println(err)
	if err !=nil {
		panic(err)
	}
	fmt.Println(label)
	return QueryTrackPayload{Label: label}
}

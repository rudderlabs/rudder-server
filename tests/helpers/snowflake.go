package helpers

import (
	"database/sql"
	"fmt"
	_ "github.com/snowflakedb/gosnowflake"
	"strings"
)

func querySnowflake(anonymousId string, table string, schema string, destConfig interface{}) QueryTrackPayload{
	config := destConfig.(map[string]interface{})
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
	row:=db.QueryRow(fmt.Sprintf(`select label, category, property1, property2, property3, property4, property5 from %s where ANONYMOUS_ID='%s'`,strings.ToUpper(table), anonymousId))
	var label, category, property1, property2, property3,property4,property5 string
	err = row.Scan(&label,&category,&property1,&property2,&property3,&property4,&property5)
	if err !=nil && err != sql.ErrNoRows{
		panic(err)
	}
	return QueryTrackPayload{
		Label: label,
		Category:category,
		Property1:property1,
		Property2:property2,
		Property3:property3,
		Property4:property4,
		Property5:property5,
	}
}

package helpers

import (
	"database/sql"
	"fmt"
)

func queryRS(anonymousId string, table string, namespace string, destConfig interface{}) QueryTrackPayload {
	config := destConfig.(map[string]interface{})
	fmt.Println(config)
	if _,ok:= config["user"]; !ok {
		panic("user not found")
	}
	if _,ok:= config["password"]; !ok {
		panic("password not found")
	}
	if _,ok:= config["host"]; !ok {
		panic("account not found")
	}
	if _,ok:= config["database"]; !ok {
		panic("database not found")
	}
	if _,ok:= config["port"]; !ok {
		panic("warehouse not found")
	}
	username:=config["user"]
	password:=config["password"]
	host:=config["host"]
	port:=config["port"]
	dbName:=config["database"]
	url := fmt.Sprintf("sslmode=require user=%v password=%v host=%v port=%v dbname=%v",
		username,
		password,
		host,
		port,
		dbName)

	var err error
	var db *sql.DB
	db, err = sql.Open("postgres", url)
	if err!=nil {
		panic(err)
	}
	row:=db.QueryRow(fmt.Sprintf(`select label from %s where anonymous_id='%s'`,fmt.Sprintf("%s.%s",namespace,table), anonymousId))
	var label string
	err = row.Scan(&label)
	fmt.Println(err)
	if err !=nil {
		panic(err)
	}
	fmt.Println(label)
	return QueryTrackPayload{Label: label}
}

package populateUserID

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGKILL, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	connectionString := GetConnectionString()
	sqlDB, err := sql.Open("postgres", connectionString)
	if err != nil {
		fmt.Println("Error opening DB: ", err)
		return
	}
	defer sqlDB.Close()

	err = sqlDB.Ping()
	if err != nil {
		fmt.Println("Error pinging DB: ", err)
		return
	}

	sqlQuery := fmt.Sprintf(`SELECT id FROM wh_staging_files WHERE user_id IS NULL`)
	rows, err := sqlDB.QueryContext(ctx, sqlQuery)
	if err != nil {
		fmt.Println("Error querying DB: ", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id, uploadID int
		err = rows.Scan(&id)
		if err != nil {
			fmt.Println("Error scanning rows: ", err)
			return
		}
		fmt.Println(id)
		uploadIdRow := sqlDB.QueryRowContext(ctx, fmt.Sprintf(`SELECT upload_id from wh_uploads where start_staging_file_id <= %[1]d and end_staging_file_id >= %[2]d`, id, id))
		err := uploadIdRow.Scan(&uploadID)
		if err != nil && err != sql.ErrNoRows {
			fmt.Println("Error scanning uploadID: ", err)
			continue
		}
		res, err := sqlDB.ExecContext(ctx, fmt.Sprintf(`UPDATE wh_staging_files SET upload = %[1]q WHERE id = %[2]q`, uploadID, id))
		if err != nil {
			fmt.Println("Error updating staging file: ", err)
			continue
		}
		fmt.Println(res.RowsAffected())
	}

}

func GetConnectionString() string {
	host := getEnvString("DB.host", "localhost")
	user := getEnvString("DB.user", "rudder")
	dbname := getEnvString("DB.name", "jobsdb")
	port := getEnvString("DB.port", "6432")
	password := getEnvString("DB.password", "password")
	sslmode := getEnvString("DB.sslMode", "disable")
	return fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=%s",
		host, port, user, password, dbname, sslmode)
}

func getEnvString(envVar, defaultValue string) string {
	v, ok := os.LookupEnv(envVar)
	if !ok {
		return defaultValue
	}
	return v
}

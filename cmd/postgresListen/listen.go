package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func main() {
	log := logger.NewLogger().Child("postgresListen")

	pool, err := pgxpool.New(context.Background(), misc.GetConnectionString())
	if err != nil {
		fmt.Fprintln(os.Stderr, "Unable to connect to database:", err)
		os.Exit(1)
	}
	defer pool.Close()
	log.Info("acquired connection pool")

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error acquiring connection:", err)
		os.Exit(1)
	}
	defer conn.Release()
	log.Info("acquired connection")

	_, err = conn.Exec(context.Background(), "listen counts")
	if err != nil {
		log.Error(os.Stderr, "Error listening to counts channel:", err)
		os.Exit(1)
	}
	log.Info("listening on counts channel...")

	for {
		notification, err := conn.Conn().WaitForNotification(context.Background())
		if err != nil {
			log.Error(os.Stderr, "Error waiting for notification:", err)
			os.Exit(1)
		}

		log.Info("PID:", notification.PID, "Channel:", notification.Channel, "Payload:", notification.Payload)
	}
}

package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func main() {
	log := logger.NewLogger().Child("postgresListen")

	pool, err := pgxpool.New(context.Background(), misc.GetConnectionString())
	if err != nil {
		log.Fatal("Unable to connect to database:", err)
	}
	defer pool.Close()
	log.Info("acquired connection pool")

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		log.Fatal("Error acquiring connection:", err)
	}
	defer conn.Release()
	log.Info("acquired connection")

	_, err = conn.Exec(context.Background(), "listen counts")
	if err != nil {
		log.Fatal("Error listening to counts channel:", err)
	}
	log.Info("listening on counts channel...")

	for {
		notification, err := conn.Conn().WaitForNotification(context.Background())
		if err != nil {
			log.Fatal("Error waiting for notification:", err)
		}

		log.Info("PID:", notification.PID, "Channel:", notification.Channel, "Payload:", notification.Payload)
	}
}

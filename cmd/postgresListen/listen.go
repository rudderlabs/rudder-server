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
		log.Error("Unable to connect to database:", err)
		return
	}
	defer pool.Close()
	log.Info("acquired connection pool")

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		log.Error("Error acquiring connection:", err)
		return
	}
	defer conn.Release()
	log.Info("acquired connection")

	_, err = conn.Exec(context.Background(), "listen counts")
	if err != nil {
		log.Error("Error listening to counts channel:", err)
		return
	}
	log.Info("listening on counts channel...")

	for {
		notification, err := conn.Conn().WaitForNotification(context.Background())
		if err != nil {
			log.Error("Error waiting for notification:", err)
			return
		}

		log.Info("PID:", notification.PID, "Channel:", notification.Channel, "Payload:", notification.Payload)
	}
}

package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-go-kit/throttling"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGKILL, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var (
		key         = getEnvString("KEY", rand.UniqueString(10))
		rate        = getEnvInt64("RATE", 10000)
		window      = getEnvInt64("WINDOW", 60)
		duration    = getEnvInt64("DURATION", 180) // 3 minutes
		concurrency = getEnvInt64("CONCURRENCY", 5000)
		redisAddr   = getEnvString("REDIS_ADDR", "localhost:6379")
	)

	log.Println("Trying to connect to Redis at:", redisAddr)

	rc := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer func() {
		if err := rc.Close(); err != nil {
			log.Printf("Error while closing Redis client: %v", err)
		}
	}()
	status := rc.Ping(ctx)
	if status.Err() != nil {
		log.Printf("Error while pinging Redis: %v", status.Err())
		return
	}
	if !strings.Contains(status.String(), "PONG") {
		log.Printf(`Unexpected response from Redis, got %q instead of "PONG"`, status.String())
		return
	}

	l, err := throttling.New(throttling.WithRedisSortedSet(rc))
	if err != nil {
		log.Printf("Could not create throttling client: %v", err)
		return
	}

	log.Println("Initiating benchmark on key:", key)

	var (
		wg               sync.WaitGroup
		start            = time.Now()
		numberOfRequests int32
		maxNoOfRoutines  = make(chan struct{}, concurrency)
		runFor           = time.NewTimer(time.Duration(duration) * time.Second)
	)
	defer func() {
		elapsed := time.Since(start)
		log.Println("Number of requests:", numberOfRequests)
		log.Println("Elapsed time:", elapsed)
		log.Println("RPS:", float64(numberOfRequests)/elapsed.Seconds())
	}()
	defer func() {
		log.Println("Waiting on all goroutines to finish...")
		wg.Wait()
	}()
	for {
		select {
		case <-ctx.Done():
			log.Println("Context done:", ctx.Err())
			return
		case <-runFor.C:
			log.Println("Timer triggered")
			return
		default:
			select {
			case <-ctx.Done():
				log.Println("Context done:", ctx.Err())
				return
			case <-runFor.C:
				log.Println("Timer triggered")
				return
			case maxNoOfRoutines <- struct{}{}:
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-maxNoOfRoutines }()
				defer func() { atomic.AddInt32(&numberOfRequests, 1) }()
				_, _, err := l.Allow(ctx, 1, rate, window, key)
				if err != nil {
					if !errors.Is(err, context.Canceled) {
						log.Printf("Could not limit: %v", err)
					}
					return
				}
			}()
		}
	}
}

func getEnvString(envVar, defaultValue string) string {
	v, ok := os.LookupEnv(envVar)
	if !ok {
		return defaultValue
	}
	return v
}

func getEnvInt64(envVar string, defaultValue int64) int64 {
	v, ok := os.LookupEnv(envVar)
	if !ok {
		return defaultValue
	}
	i, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		panic(err)
	}
	return i
}

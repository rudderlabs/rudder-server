# ratelimiter

[![go-doc](https://godoc.org/github.com/coinpaprika/ratelimiter?status.svg)](https://godoc.org/github.com/coinpaprika/ratelimiter)
[![Go Report Card](https://goreportcard.com/badge/github.com/coinpaprika/ratelimiter)](https://goreportcard.com/report/github.com/coinpaprika/ratelimiter)

Simple rate limiter for any resources inspired by Cloudflare's approach: [How we built rate limiting capable of scaling to millions of domains.](https://blog.cloudflare.com/counting-things-a-lot-of-different-things/)

## Usage

### Getting started

```go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/coinpaprika/ratelimiter"
)

func main() {
	limitedKey := "key"
	windowSize := 1 * time.Minute

	dataStore := ratelimiter.NewMapLimitStore(2*windowSize, 10*time.Second) // create map data store for rate limiter and set each element's expiration time to 2*windowSize and old data flush interval to 10*time.Second

	var maxLimit int64 = 5
	rateLimiter := ratelimiter.New(dataStore, maxLimit, windowSize) // allow 5 requests per windowSize (1 minute)

	for i := 0; i < 10; i++ {
		limitStatus, err := rateLimiter.Check(limitedKey)
		if err != nil {
			log.Fatal(err)
		}
		if limitStatus.IsLimited {
			fmt.Printf("too high rate for key: %s: rate: %f, limit: %d\nsleep: %s", limitedKey, limitStatus.CurrentRate, maxLimit, *limitStatus.LimitDuration)
			time.Sleep(*limitStatus.LimitDuration)
		} else {
			err := rateLimiter.Inc(limitedKey)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}
```

### Rate-limit IP requests in http middleware

```go
func rateLimitMiddleware(rateLimiter *ratelimiter.RateLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			remoteIP := GetRemoteIP([]string{"X-Forwarded-For", "RemoteAddr", "X-Real-IP"}, 0, r)
			key := fmt.Sprintf("%s_%s_%s", remoteIP, r.URL.String(), r.Method)

			limitStatus, err := rateLimiter.Check(key)
			if err != nil {
				// if rate limit error then pass the request
				next.ServeHTTP(w, r)
			}
			if limitStatus.IsLimited {
				w.WriteHeader(http.StatusTooManyRequests)
				return
			} else {
				rateLimiter.Inc(key)
			}

			next.ServeHTTP(w, r)
		})
	}
}

func hello(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello, %q", html.EscapeString(r.URL.Path))
}

func main() {
	windowSize := 1 * time.Minute
	dataStore := ratelimiter.NewMapLimitStore(2*windowSize, 10*time.Second) // create map data store for rate limiter and set each element's expiration time to 2*windowSize and old data flush interval to 10*time.Second
	rateLimiter := ratelimiter.New(dataStore, 5, windowSize)                // allow 5 requests per windowSize (1 minute)

	rateLimiterHandler := rateLimitMiddleware(rateLimiter)
	helloHandler := http.HandlerFunc(hello)
	http.Handle("/", rateLimiterHandler(helloHandler))

	log.Fatal(http.ListenAndServe(":8080", nil))

}
```
See full [example](./examples/http_middleware/http_middleware.go)

### Implement your own limit data store
To use custom data store (memcached, Redis, MySQL etc.) you just need to implement [LimitStore](./limit_store.go) interface:
```go
type FakeDataStore struct{}

func (f FakeDataStore) Inc(key string, window time.Time) error {
	return nil
}

func (f FakeDataStore) Get(key string, previousWindow, currentWindow time.Time) (prevValue int64, currValue int64, err error) {
	return 0, 0, nil
}
// ...
rateLimiter := ratelimiter.New(FakeDataStore{}, maxLimit, windowSize)
```

## Examples 

Check out the [examples](./examples) directory.


## License

ratelimiter is available under the MIT license. See the [LICENSE file](./LICENSE.md) for more info.

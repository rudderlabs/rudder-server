# restrictor

This is a rate limiter using sliding window counter algorithm implemented with Golang.

Other libraries which uses 'token bucket'(in 'time/rate' standard library) only support 'request per second' senario. This library can support 'xx request per xx minutes(or hours or days)'.

## How to get
```
go get github.com/EagleChen/restrictor
```

## How to use
```
// first, create a 'store' to store various limiters for each key
store, err := NewMemoryStore()
// in case there are many backend servers using one central limiter store
// use 'NewRedisStore' instead

// second, create restrictor
// 500 request every 5 minutes
// 60 is the internal bucket number. This number will affect the deviation
// Usually pick some number from 60 to 100.
r := NewRestrictor(5 * time.Minute, 500, 60, store)

// third, check limit
// yourKey might be an IP addr, a user id, etc.
if r.LimitReached(yourKey) {
    // limit is reached, notify user
} else {
    // go on do the real job
}
```

## What is the 'internal bucket number' for?
Under high load, requests(limit check times) in a window may be very large. It's not efficient to store all the request with timestamp. Instead, this implementation divides a window into many 'buckets'. Each 'bucket' is to record the request count in a short time(during rate calculation, all the requests in one bucket seems like happening at the same time). Say there are 10 billion requests in a window, in case of 60 buckets, it's enough to use 60 numbers to record the requests. It's much more CPU and Memory efficient. 

However, this kind of 'approximation' will make the rate limiter not very accurate. Larger number of buckets means more accurate but less efficient, so please choose your 'bucket number' according to your use senario.

## Other Related Resource
Related Blog Post(in Chinese): [访问频率限制——窗口相关算法](https://www.jianshu.com/p/f023fabad69b)
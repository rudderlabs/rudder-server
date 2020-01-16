# mapmutex

mapmutex is a simple implementation to act as a group of mutex.

## What's it for?
Synchronization is needed in many cases. But in some cases, you don't want a gaint lock to block totally irrelevant actions. Instead, you need many fine-grained tiny locks to only block on same resource.

Take an example. A website have many users. Each user has a different counter. While one user want to increment the counter at the same time in different devices(say, from a pad and a phone), these increments need to happen one by one. But user A's incremntation has nothing to do with user B's incrementation, they don't have to affect each other.
This is where this package comes in. You can lock for each user (by using user id as key) without blocking other users.

## Performance
As shown by the result of benchmark(in `mutex_test.go`), it's several times faster than one giant mutex.
```
(11 times faster)
BenchmarkMutex1000_100_20_20-4          	       1	20164937908 ns/op
BenchmarkMapMutex1000_100_20_20-4       	       1	1821899222 ns/op 

(7 times faster)
BenchmarkMutex1000_20_20_20-4           	       1	19726327623 ns/op
BenchmarkMapMutex1000_20_20_20-4        	       1	2759654813 ns/op

(11 times faster)
BenchmarkMutex1000_20_40_20-4           	       1	20380128848 ns/op
BenchmarkMapMutex1000_20_40_20-4        	       1	1828899343 ns/op

(only 2 keys in map, 2 times faster)
(in case of only one key in map, it's the same as one gaint lock)
BenchmarkMutex1000_2_40_20-4            	       1	20721092007 ns/op
BenchmarkMapMutex1000_2_40_20-4         	       1	10818512020 ns/op (989 of 1000 success)

(9 times faster)
BenchmarkMutex1000_20_40_60-4           	       1	60341833247 ns/op
BenchmarkMapMutex1000_20_40_60-4        	       1	6240238975 ns/op

(11 times faster)
BenchmarkMutex10000_20_40_20-4          	       1	205493472245 ns/op
BenchmarkMapMutex10000_20_40_20-4       	       1	18677416055 ns/op
```

## How to get
```
go get github.com/EagleChen/mapmutex
```

## How to use
```
mutex := mapmutex.NewMapMutex()
if mutex.TryLock(key) { // for example, key can be user id
    // do the real job here

    mutex.Unlock(key)
}
```

TryLock itself will retry several times to aquire the lock. But in the application level, you can also try several times when the lock cannot be got.
```
got := false
for i := 0; && i < retryTimes; i++ {
    if got = mutex.TryLock(key); got {
        break
    }
}
if got {
    // do the real job here

    mutex.Unlock(key)
}
```

## How to tune
1. Use `NewCustomizedMapMutex` to customize how hard 'TryLock' will try to get the lock. The parameters controls how many times to try, how long to wait before another try when failing to aquire the lock, etc. They may be very different for various use cases.

2. Change some source code for your use case. For general use, `map[interface{}]interface{}` is used for storing 'locks'. But it can be changed to `map[int]bool` if your `key` is `int` and `map[string]bool` if you `key` is `string`. As far as i know, this trick will improve the performance, a little bit.
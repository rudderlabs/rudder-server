# go-lock

[![GoDev](https://img.shields.io/badge/go.dev-doc-007d9c?style=flat-square&logo=read-the-docs)](https://pkg.go.dev/github.com/viney-shih/go-lock?tab=doc)
[![Build Status](https://travis-ci.com/viney-shih/go-lock.svg?branch=master)](https://travis-ci.com/github/viney-shih/go-lock)
[![Go Report Card](https://goreportcard.com/badge/github.com/viney-shih/go-lock)](https://goreportcard.com/report/github.com/viney-shih/go-lock)
[![Codecov](https://codecov.io/gh/viney-shih/go-lock/branch/master/graph/badge.svg)](https://codecov.io/gh/viney-shih/go-lock)
[![Coverage Status](https://coveralls.io/repos/github/viney-shih/go-lock/badge.svg?branch=master)](https://coveralls.io/github/viney-shih/go-lock?branch=master)
[![Sourcegraph](https://sourcegraph.com/github.com/viney-shih/go-lock/-/badge.svg)](https://sourcegraph.com/github.com/viney-shih/go-lock?badge)
[![License](http://img.shields.io/badge/License-Apache_2-red.svg?style=flat)](http://www.apache.org/licenses/LICENSE-2.0)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fviney-shih%2Fgo-lock.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fviney-shih%2Fgo-lock?ref=badge_shield)
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go#utilities)

<p align="center">
  <img src="assets/logo.png" title="go-lock" />
</p>

**go-lock** is a Golang library implementing an effcient read-write lock with the following built-in mechanism:
- Mutex with timeout mechanism
- Trylock
- No-starve read-write solution

Native `sync/Mutex` and `sync/RWMutex` are very powerful and reliable. However, it became a disaster if the lock was not released as expected. Or, someone was holding the lock too long at the peak time leading whole system blocked. Dealing with those cases, **go-lock** implements `TryLock`, `TryLockWithTimeout` and `TryLockWithContext` function in addition to Lock and Unlock. It provides flexibility to control the resources.

## Installation

```sh
go get github.com/viney-shih/go-lock
```

## Example
```go
package main

import (
	"fmt"
	"sync/atomic"
	"time"

	lock "github.com/viney-shih/go-lock"
)

func main() {
	// set RWMutex with CAS mechanism (CASMutex).
	var rwMut lock.RWMutex = lock.NewCASMutex()
	// set default value
	count := int32(0)

	// block here
	rwMut.Lock()
	go func() {
		time.Sleep(50 * time.Millisecond)
		fmt.Println("Now is", atomic.AddInt32(&count, 1)) // Now is 1
		rwMut.Unlock()
	}()

	// waiting for previous goroutine releasing the lock, and locking it again
	rwMut.Lock()
	fmt.Println("Now is", atomic.AddInt32(&count, 2)) // Now is 3

	// TryLock without blocking
	// Return false, because the lock is not released.
	fmt.Println("Return", rwMut.TryLock())

	// RTryLockWithTimeout without blocking
	// Return false, because the lock is not released.
	fmt.Println("Return", rwMut.RTryLockWithTimeout(50*time.Millisecond))

	// TryLockWithContext without blocking
	ctx, cancel := context.WithTimeout(context.TODO(), 50*time.Millisecond)
	defer cancel()
	// Return false, because the lock is not released.
	fmt.Println("Return", rwMut.TryLockWithContext(ctx))

	// release the lock in the end.
	rwMut.Unlock()

	// Output:
	// Now is 1
	// Now is 3
	// Return false
	// Return false
	// Return false
}
```

- [More examples](./cas_test.go)
- [Full API documentation](https://pkg.go.dev/github.com/viney-shih/go-lock?tab=doc)

## Benchmarks
- Run on MacBook Pro (Retina, 15-inch, Mid 2015) 2.5 GHz Quad-Core Intel Core i7 16 GB 1600 MHz DDR3 using Go 1.15.2
- Run with 1, 2, 4, 8 and 16 cpu to show it scales well...16 is double the # of logical cores on this machine.

```
go test -cpu=1,2,4,8,16 -bench=. -benchmem=true

goos: darwin
goarch: amd64
pkg: github.com/viney-shih/go-lock

(sync.RWMutex)
BenchmarkRWMutexLock                       	42591765	        27.4 ns/op	       0 B/op	       0 allocs/op
BenchmarkRWMutexLock-2                     	42713179	        27.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkRWMutexLock-4                     	44348323	        27.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkRWMutexLock-8                     	44135148	        27.9 ns/op	       0 B/op	       0 allocs/op
BenchmarkRWMutexLock-16                    	43566165	        27.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkConcurrentRWMutexLock             	42971319	        27.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkConcurrentRWMutexLock-2           	20131005	        57.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkConcurrentRWMutexLock-4           	10752337	       115 ns/op	       0 B/op	       0 allocs/op
BenchmarkConcurrentRWMutexLock-8           	11434335	       105 ns/op	       0 B/op	       0 allocs/op
BenchmarkConcurrentRWMutexLock-16          	10495626	       109 ns/op	       0 B/op	       0 allocs/op
BenchmarkConcurrent50RWMutexLock           	27979630	        42.8 ns/op	       0 B/op	       0 allocs/op
BenchmarkConcurrent50RWMutexLock-2         	13037742	        86.6 ns/op	       0 B/op	       0 allocs/op
BenchmarkConcurrent50RWMutexLock-4         	 9143397	       134 ns/op	       0 B/op	       0 allocs/op
BenchmarkConcurrent50RWMutexLock-8         	 8335652	       139 ns/op	       0 B/op	       0 allocs/op
BenchmarkConcurrent50RWMutexLock-16        	 7876855	       150 ns/op	       0 B/op	       0 allocs/op

(ChanMutex)
BenchmarkChanMutexLock                     	22619928	        51.8 ns/op	       0 B/op	       0 allocs/op
BenchmarkChanMutexLock-2                   	22769630	        51.6 ns/op	       0 B/op	       0 allocs/op
BenchmarkChanMutexLock-4                   	23096103	        51.7 ns/op	       0 B/op	       0 allocs/op
BenchmarkChanMutexLock-8                   	22627267	        51.1 ns/op	       0 B/op	       0 allocs/op
BenchmarkChanMutexLock-16                  	23092266	        51.7 ns/op	       0 B/op	       0 allocs/op
BenchmarkConcurrentChanMutexLock           	23422556	        51.8 ns/op	       0 B/op	       0 allocs/op
BenchmarkConcurrentChanMutexLock-2         	 6101949	       201 ns/op	       0 B/op	       0 allocs/op
BenchmarkConcurrentChanMutexLock-4         	 5882083	       200 ns/op	       0 B/op	       0 allocs/op
BenchmarkConcurrentChanMutexLock-8         	 5827183	       211 ns/op	       0 B/op	       0 allocs/op
BenchmarkConcurrentChanMutexLock-16        	 5577098	       215 ns/op	       0 B/op	       0 allocs/op
BenchmarkChanMutexTryLock                  	22272500	        51.8 ns/op	       0 B/op	       0 allocs/op
BenchmarkChanMutexTryLock-2                	23004806	        52.2 ns/op	       0 B/op	       0 allocs/op
BenchmarkChanMutexTryLock-4                	22461870	        52.3 ns/op	       0 B/op	       0 allocs/op
BenchmarkChanMutexTryLock-8                	22901328	        53.8 ns/op	       0 B/op	       0 allocs/op
BenchmarkChanMutexTryLock-16               	22334739	        55.4 ns/op	       0 B/op	       0 allocs/op
BenchmarkConcurrentChanMutexTryLock        	22112250	        53.9 ns/op	       0 B/op	       0 allocs/op
BenchmarkConcurrentChanMutexTryLock-2      	13806072	        85.0 ns/op	       0 B/op	       0 allocs/op
BenchmarkConcurrentChanMutexTryLock-4      	45892635	        32.6 ns/op	       0 B/op	       0 allocs/op
BenchmarkConcurrentChanMutexTryLock-8      	396310569	         2.86 ns/op	       0 B/op	       0 allocs/op
BenchmarkConcurrentChanMutexTryLock-16     	512990590	         2.35 ns/op	       0 B/op	       0 allocs/op

(CASMutex)
BenchmarkCASMutexLock                      	 6160122	       186 ns/op	      96 B/op	       1 allocs/op
BenchmarkCASMutexLock-2                    	 7507022	       156 ns/op	      96 B/op	       1 allocs/op
BenchmarkCASMutexLock-4                    	 7646648	       156 ns/op	      96 B/op	       1 allocs/op
BenchmarkCASMutexLock-8                    	 7559616	       156 ns/op	      96 B/op	       1 allocs/op
BenchmarkCASMutexLock-16                   	 7576237	       158 ns/op	      96 B/op	       1 allocs/op
BenchmarkConcurrentCASMutexLock            	 6375879	       185 ns/op	      96 B/op	       1 allocs/op
BenchmarkConcurrentCASMutexLock-2          	 1690890	       706 ns/op	     251 B/op	       3 allocs/op
BenchmarkConcurrentCASMutexLock-4          	 1677104	       714 ns/op	     255 B/op	       3 allocs/op
BenchmarkConcurrentCASMutexLock-8          	 1808582	       639 ns/op	     255 B/op	       3 allocs/op
BenchmarkConcurrentCASMutexLock-16         	 1918422	       622 ns/op	     255 B/op	       3 allocs/op
BenchmarkConcurrent50CASMutexLock          	 5650274	       210 ns/op	      96 B/op	       1 allocs/op
BenchmarkConcurrent50CASMutexLock-2        	 1698381	       707 ns/op	     247 B/op	       3 allocs/op
BenchmarkConcurrent50CASMutexLock-4        	 1697070	       707 ns/op	     255 B/op	       3 allocs/op
BenchmarkConcurrent50CASMutexLock-8        	 1809859	       655 ns/op	     255 B/op	       3 allocs/op
BenchmarkConcurrent50CASMutexLock-16       	 1801652	       646 ns/op	     256 B/op	       4 allocs/op
BenchmarkCASMutexTryLock                   	 6593228	       182 ns/op	      96 B/op	       1 allocs/op
BenchmarkCASMutexTryLock-2                 	 7703084	       154 ns/op	      96 B/op	       1 allocs/op
BenchmarkCASMutexTryLock-4                 	 7790750	       151 ns/op	      96 B/op	       1 allocs/op
BenchmarkCASMutexTryLock-8                 	 7756263	       152 ns/op	      96 B/op	       1 allocs/op
BenchmarkCASMutexTryLock-16                	 7741186	       152 ns/op	      96 B/op	       1 allocs/op
BenchmarkConcurrentCASMutexTryLock         	 6509828	       189 ns/op	      96 B/op	       1 allocs/op
BenchmarkConcurrentCASMutexTryLock-2       	14058355	        82.2 ns/op	      17 B/op	       0 allocs/op
BenchmarkConcurrentCASMutexTryLock-4       	16875369	        71.4 ns/op	       8 B/op	       0 allocs/op
BenchmarkConcurrentCASMutexTryLock-8       	10985379	       109 ns/op	       4 B/op	       0 allocs/op
BenchmarkConcurrentCASMutexTryLock-16      	12405752	       104 ns/op	       1 B/op	       0 allocs/op
BenchmarkConcurrent50CASMutexTryLock       	 5631146	       204 ns/op	      96 B/op	       1 allocs/op
BenchmarkConcurrent50CASMutexTryLock-2     	 6518946	       182 ns/op	      61 B/op	       0 allocs/op
BenchmarkConcurrent50CASMutexTryLock-4     	 6937346	       174 ns/op	      43 B/op	       0 allocs/op
BenchmarkConcurrent50CASMutexTryLock-8     	 6493034	       182 ns/op	      39 B/op	       0 allocs/op
BenchmarkConcurrent50CASMutexTryLock-16    	 6177854	       202 ns/op	      41 B/op	       0 allocs/op
PASS

```

## References
- https://github.com/golang/go/issues/6123
- https://github.com/LK4D4/trylock
- https://github.com/OneOfOne/go-utils/tree/master/sync
- https://github.com/lrita/gosync
- https://github.com/google/netstack/blob/master/tmutex/tmutex.go
- https://github.com/subchen/go-trylock

## License
[Apache-2.0](https://opensource.org/licenses/Apache-2.0)


[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fviney-shih%2Fgo-lock.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Fviney-shih%2Fgo-lock?ref=badge_large)

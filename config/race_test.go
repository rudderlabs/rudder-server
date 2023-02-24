package config

import (
	"math/rand"
	"testing"
	"time"
)

func TestIntRace(t *testing.T) {
	var myInt int

	conf := New()
	conf.RegisterIntConfigVariable(1, &myInt, true, 1, "key-1")

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				t.Log(myInt)
			}
		}
	}()

	runFor := time.NewTimer(100 * time.Millisecond)
loop:
	for {
		select {
		case <-runFor.C:
			close(stop)
			break loop
		default:
			conf.Set("key-1", rand.Intn(100))
		}
	}
	<-done
}

func TestAtomicIntRace(t *testing.T) {
	var myInt Atomic[int]

	conf := New()
	conf.RegisterAtomicIntConfigVar(1, &myInt, 1, "key-1")

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				t.Log(myInt.Load())
			}
		}
	}()

	runFor := time.NewTimer(100 * time.Millisecond)
loop:
	for {
		select {
		case <-runFor.C:
			close(stop)
			break loop
		default:
			conf.Set("key-1", rand.Intn(100))
		}
	}
	<-done
}

package main

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
)

func main() {
	fmt.Println("Hello, 世界")

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
		}
	}()

	g, _ := errgroup.WithContext(context.Background())

	g.Go(func() error {
		time.Sleep(time.Second)
		panic("random panic")

		return nil
	})

	g.Wait()
}

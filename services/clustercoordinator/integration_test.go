package clustercoordinator_test

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/ory/dockertest"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

var (
	hold bool
)

func TestMain(m *testing.M) {
	flag.BoolVar(&hold, "hold", false, "hold environment clean-up after test execution until Ctrl+C is provided")
	flag.Parse()
	// hack to make defer work, without being affected by the os.Exit in TestMain
	os.Exit(run(m))
}

func run(m *testing.M) int {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
	fmt.Println("Connected to docker")
	resourceETCD, err := pool.Run("bitnami/etcd", "3.5.2", []string{})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	fmt.Println("Started resource")
	defer func() {
		if err := pool.Purge(resourceETCD); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

	os.Setenv("ETCD_HOST", "127.0.0.1:"+resourceETCD.GetPort("2379/tcp"))
	os.Setenv("RELEASE_NAME", "multitenant_test")
	fmt.Println("127.0.0.1:" + resourceETCD.GetPort("2379/tcp"))
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:            []string{"127.0.0.1:" + resourceETCD.GetPort("2379/tcp")},
		DialTimeout:          40 * time.Second,
		DialKeepAliveTime:    60 * time.Second,
		DialKeepAliveTimeout: 20 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithBlock(), // block until the underlying connection is up
		},
	})
	if err != nil {
		fmt.Println(err)
	}
	ctx := context.TODO()
	_, err = cli.Get(ctx, "test")
	fmt.Println(err)
	// if err := pool.Retry(func() error {
	// 	var err error
	// 	fmt.Println("127.0.0.1:" + resourceETCD.GetPort("2379/tcp"))
	// 	cli, err := clientv3.New(clientv3.Config{
	// 		Endpoints: []string{"127.0.0.1:" + resourceETCD.GetPort("2379/tcp")},
	// 	})
	// 	if err != nil {
	// 		fmt.Println(err)
	// 		return err
	// 	}
	// 	ctx := context.TODO()
	// 	_, err = cli.Get(ctx, "test")
	// 	fmt.Println(err)
	// 	return err

	// }); err != nil {
	// 	log.Fatalf("Could not connect to docker: %s", err)
	// }
	return 1
}

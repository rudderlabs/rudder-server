package etcd

import (
	"fmt"
	"strconv"
	"time"

	"github.com/ory/dockertest/v3"
	etcd "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

type cleaner interface {
	Cleanup(func())
	Log(...interface{})
}

type Resource struct {
	Client *etcd.Client
	Hosts  []string
	Port   int
}

func Setup(pool *dockertest.Pool, cln cleaner) (*Resource, error) {
	etcdImage := "bitnami/etcd"
	container, err := pool.Run(etcdImage, "3.5", []string{
		"ALLOW_NONE_AUTHENTICATION=yes",
	})
	if err != nil {
		return nil, fmt.Errorf("could not create container: %v", err)
	}
	cln.Cleanup(func() {
		if err := pool.Purge(container); err != nil {
			cln.Log(fmt.Errorf("could not purge ETCD resource: %v", err))
		}
	})

	var (
		etcdClient *etcd.Client
		etcdHosts  []string
		etcdPort   int

		etcdPortStr = container.GetPort("2379/tcp")
	)
	etcdPort, err = strconv.Atoi(etcdPortStr)
	if err != nil {
		return nil, fmt.Errorf("could not convert port %q to int: %v", etcdPortStr, err)
	}

	etcdHosts = []string{"http://localhost:" + etcdPortStr}
	err = pool.Retry(func() (err error) {
		etcdClient, err = etcd.New(etcd.Config{
			Endpoints: etcdHosts,
			DialOptions: []grpc.DialOption{
				grpc.WithBlock(), // block until the underlying connection is up
			},
			DialTimeout: 10 * time.Second,
		})
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("could not connect to dockerized ETCD: %v", err)
	}

	return &Resource{
		Client: etcdClient,
		Hosts:  etcdHosts,
		Port:   etcdPort,
	}, nil
}

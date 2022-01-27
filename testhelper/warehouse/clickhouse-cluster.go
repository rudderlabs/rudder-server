package warehouse_test

import (
	"database/sql"
	"fmt"
	"github.com/ory/dockertest"
	dc "github.com/ory/dockertest/docker"
	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"log"
	"os"
)

type ClickHouseClusterTest struct {
	Network      *dc.Network
	Zookeeper    *dockertest.Resource
	Clickhouse01 *dockertest.Resource
	Clickhouse02 *dockertest.Resource
	Clickhouse03 *dockertest.Resource
	Clickhouse04 *dockertest.Resource
	Credentials  *clickhouse.CredentialsT
	DB           *sql.DB
	EventsMap    EventsCountMap
	WriteKey     string
}

// SetWHClickHouseClusterDestination setup warehouse clickhouse cluster mode destination
func SetWHClickHouseClusterDestination(pool *dockertest.Pool) (cleanup func()) {
	Test.CHClusterTest = &ClickHouseClusterTest{
		WriteKey: randString(27),
		Credentials: &clickhouse.CredentialsT{
			Host:          "localhost",
			User:          "rudder",
			Password:      "rudder-password",
			DBName:        "rudderdb",
			Secure:        "false",
			SkipVerify:    "true",
			TLSConfigName: "",
		},
		EventsMap: EventsCountMap{
			"identifies":    1,
			"users":         1,
			"tracks":        1,
			"product_track": 1,
			"pages":         1,
			"screens":       1,
			"aliases":       1,
			"groups":        1,
			"gateway":       6,
			"batchRT":       8,
		},
	}
	chClusterTest := Test.CHClusterTest
	credentials := chClusterTest.Credentials
	cleanup = func() {}

	pwd, err := os.Getwd()
	if err != nil {
		panic(fmt.Errorf("Could not get working directory: %w", err))
	}

	var chSetupError error
	if chClusterTest.Network, err = pool.Client.CreateNetwork(dc.CreateNetworkOptions{
		Name: "clickhouse-network",
		IPAM: &dc.IPAMOptions{
			Config: []dc.IPAMConfig{
				{
					Subnet: "172.23.0.0/24",
				},
			},
		},
	}); err != nil {
		chSetupError = err
		log.Println("Could not create clickhouse cluster network: %w", err)
	}

	if chClusterTest.Zookeeper, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "zookeeper",
		Tag:        "3.5",
		Hostname:   "clickhouse-zookeeper",
		Name:       "clickhouse-zookeeper",
	}); err != nil {
		chSetupError = err
		log.Println("Could not create clickhouse cluster zookeeper: %w", err)
	}

	if chClusterTest.Clickhouse01, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "yandex/clickhouse-server",
		Tag:        "21-alpine",
		Hostname:   "clickhouse01",
		Name:       "clickhouse01",
		PortBindings: map[dc.Port][]dc.PortBinding{
			"8123": {{HostIP: "127.0.0.1", HostPort: "8123"}},
			"9000": {{HostIP: "127.0.0.1", HostPort: "9000"}},
		},
		ExposedPorts: []string{"8123", "9000"},
		Mounts:       []string{fmt.Sprintf(`%s/testdata/warehouse/clickhouse/cluster/clickhouse01:/etc/clickhouse-server`, pwd)},
		Links:        []string{"clickhouse-zookeeper"},
	}); err != nil {
		chSetupError = err
		log.Println("Could not create clickhouse cluster 1: %w", err)
	}
	if chClusterTest.Clickhouse02, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "yandex/clickhouse-server",
		Tag:        "21-alpine",
		Hostname:   "clickhouse02",
		Name:       "clickhouse02",
		Mounts:     []string{fmt.Sprintf(`%s/testdata/warehouse/clickhouse/cluster/clickhouse02:/etc/clickhouse-server`, pwd)},
		Links:      []string{"clickhouse-zookeeper"},
	}); err != nil {
		chSetupError = err
		log.Println("Could not create clickhouse cluster 2: %w", err)
	}
	if chClusterTest.Clickhouse03, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "yandex/clickhouse-server",
		Tag:        "21-alpine",
		Hostname:   "clickhouse03",
		Name:       "clickhouse03",
		Mounts:     []string{fmt.Sprintf(`%s/testdata/warehouse/clickhouse/cluster/clickhouse03:/etc/clickhouse-server`, pwd)},
		Links:      []string{"clickhouse-zookeeper"},
	}); err != nil {
		chSetupError = err
		log.Println("Could not create clickhouse cluster 3: %w", err)
	}
	if chClusterTest.Clickhouse04, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "yandex/clickhouse-server",
		Tag:        "21-alpine",
		Hostname:   "clickhouse04",
		Name:       "clickhouse04",
		Mounts:     []string{fmt.Sprintf(`%s/testdata/warehouse/clickhouse/cluster/clickhouse04:/etc/clickhouse-server`, pwd)},
		Links:      []string{"clickhouse-zookeeper"},
	}); err != nil {
		chSetupError = err
		log.Println("Could not create clickhouse cluster 4: %w", err)
	}

	if chClusterTest.Network != nil {
		if chClusterTest.Zookeeper != nil {
			if err = pool.Client.ConnectNetwork(chClusterTest.Network.ID, dc.NetworkConnectionOptions{
				Container: chClusterTest.Zookeeper.Container.Name,
				EndpointConfig: &dc.EndpointConfig{
					IPAddress: "172.23.0.10",
				},
			}); err != nil {
				chSetupError = err
				log.Println("Could not configure clickhouse clutser zookeeper network: %w", err)
			}
		}

		if chClusterTest.Clickhouse01 != nil {
			if err = pool.Client.ConnectNetwork(chClusterTest.Network.ID, dc.NetworkConnectionOptions{
				Container: chClusterTest.Clickhouse01.Container.Name,
				EndpointConfig: &dc.EndpointConfig{
					IPAddress: "172.23.0.11",
				},
			}); err != nil {
				chSetupError = err
				log.Println("Could not configure clickhouse cluster 1 network: %w", err)
			}
		}
		if chClusterTest.Clickhouse02 != nil {
			if err = pool.Client.ConnectNetwork(chClusterTest.Network.ID, dc.NetworkConnectionOptions{
				Container: chClusterTest.Clickhouse02.Container.Name,
				EndpointConfig: &dc.EndpointConfig{
					IPAddress: "172.23.0.12",
				},
			}); err != nil {
				chSetupError = err
				log.Println("Could not configure clickhouse cluster 2 network: %w", err)
			}
		}
		if chClusterTest.Clickhouse03 != nil {
			if err = pool.Client.ConnectNetwork(chClusterTest.Network.ID, dc.NetworkConnectionOptions{
				Container: chClusterTest.Clickhouse03.Container.Name,
				EndpointConfig: &dc.EndpointConfig{
					IPAddress: "172.23.0.13",
				},
			}); err != nil {
				chSetupError = err
				log.Println("Could not configure clickhouse cluster 3 network: %w", err)
			}
		}
		if chClusterTest.Clickhouse04 != nil {
			if err = pool.Client.ConnectNetwork(chClusterTest.Network.ID, dc.NetworkConnectionOptions{
				Container: chClusterTest.Clickhouse04.Container.Name,
				EndpointConfig: &dc.EndpointConfig{
					IPAddress: "172.23.0.14",
				},
			}); err != nil {
				chSetupError = err
				log.Println("Could not configure clickhouse cluster 4 network: %w", err)
			}
		}
	}

	purgeResources := func() {
		if chClusterTest.Zookeeper != nil {
			log.Printf("Purging clickhouse cluster zookeeper resource: %s \n", err)
			if err := pool.Purge(chClusterTest.Zookeeper); err != nil {
				log.Printf("Could not purge clickhouse cluster zookeeper resource: %s \n", err)
			}
		}
		if chClusterTest.Clickhouse01 != nil {
			log.Printf("Purging clickhouse cluster 1 resource: %s \n", err)
			if err := pool.Purge(chClusterTest.Clickhouse01); err != nil {
				log.Printf("Could not purge clickhouse cluster 1 resource: %s \n", err)
			}
		}
		if chClusterTest.Clickhouse02 != nil {
			log.Printf("Purging clickhouse cluster 2 resource: %s \n", err)
			if err := pool.Purge(chClusterTest.Clickhouse02); err != nil {
				log.Printf("Could not purge clickhouse cluster 2 resource: %s \n", err)
			}
		}
		if chClusterTest.Clickhouse03 != nil {
			log.Printf("Purging clickhouse cluster 3 resource: %s \n", err)
			if err := pool.Purge(chClusterTest.Clickhouse03); err != nil {
				log.Printf("Could not purge clickhouse cluster 3 resource: %s \n", err)
			}
		}
		if chClusterTest.Clickhouse04 != nil {
			log.Printf("Purging clickhouse cluster 4 resource: %s \n", err)
			if err := pool.Purge(chClusterTest.Clickhouse04); err != nil {
				log.Printf("Could not purge clickhouse cluster 4 resource: %s \n", err)
			}
		}
		if chClusterTest.Network != nil {
			log.Printf("Purging clickhouse cluster network resource: %s \n", err)
			if err := pool.Client.RemoveNetwork(chClusterTest.Network.ID); err != nil {
				log.Printf("Could not purge clickhouse cluster network resource: %s \n", err)
			}
		}
	}

	if chSetupError != nil {
		defer purgeResources()
		panic(fmt.Errorf("Could not create WareHouse ClickHouse Cluster: %v\n", chSetupError))
	}

	// Getting at which port the container is running
	credentials.Port = chClusterTest.Clickhouse01.GetPort("9000/tcp")

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		chClusterTest.DB, err = clickhouse.Connect(*credentials, true)
		if err != nil {
			return err
		}
		return chClusterTest.DB.Ping()
	}); err != nil {
		defer purgeResources()
		panic(fmt.Errorf("Could not connect to warehouse clickhouse cluster with error: %w\n", err))
	}
	cleanup = purgeResources
	return
}

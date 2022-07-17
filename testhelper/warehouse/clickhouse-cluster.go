package warehouse_test

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	"github.com/phayes/freeport"

	"github.com/rudderlabs/rudder-server/testhelper/rand"
	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
)

type ClickHouseClusterResource struct {
	Name        string
	HostName    string
	IPAddress   string
	Credentials *clickhouse.CredentialsT
	Port        string
	Resource    *dockertest.Resource
	DB          *sql.DB
}

type ClickHouseClusterResources []*ClickHouseClusterResource

func (resources *ClickHouseClusterTest) GetResource() *ClickHouseClusterResource {
	if len(resources.Resources) == 0 {
		panic("No such clickhouse cluster resource available.")
	}
	return resources.Resources[0]
}

type ClickHouseClusterTest struct {
	Network            *dc.Network
	Zookeeper          *dockertest.Resource
	Resources          ClickHouseClusterResources
	EventsMap          EventsCountMap
	WriteKey           string
	TableTestQueryFreq time.Duration
}

// SetWHClickHouseClusterDestination setup warehouse clickhouse cluster mode destination
func SetWHClickHouseClusterDestination(pool *dockertest.Pool) (cleanup func()) {
	Test.CHClusterTest = &ClickHouseClusterTest{
		WriteKey: rand.String(27),
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
		Resources: []*ClickHouseClusterResource{
			{
				Name:     "clickhouse01",
				HostName: "clickhouse01",
				Credentials: &clickhouse.CredentialsT{
					Host:          "localhost",
					User:          "rudder",
					Password:      "rudder-password",
					DBName:        "rudderdb",
					Secure:        "false",
					SkipVerify:    "true",
					TLSConfigName: "",
				},
			},
			{
				Name:     "clickhouse02",
				HostName: "clickhouse02",
				Credentials: &clickhouse.CredentialsT{
					Host:          "localhost",
					User:          "rudder",
					Password:      "rudder-password",
					DBName:        "rudderdb",
					Secure:        "false",
					SkipVerify:    "true",
					TLSConfigName: "",
				},
			},
			{
				Name:     "clickhouse03",
				HostName: "clickhouse03",
				Credentials: &clickhouse.CredentialsT{
					Host:          "localhost",
					User:          "rudder",
					Password:      "rudder-password",
					DBName:        "rudderdb",
					Secure:        "false",
					SkipVerify:    "true",
					TLSConfigName: "",
				},
			},
			{
				Name:     "clickhouse04",
				HostName: "clickhouse04",
				Credentials: &clickhouse.CredentialsT{
					Host:          "localhost",
					User:          "rudder",
					Password:      "rudder-password",
					DBName:        "rudderdb",
					Secure:        "false",
					SkipVerify:    "true",
					TLSConfigName: "",
				},
			},
		},
		TableTestQueryFreq: 100 * time.Millisecond,
	}
	chClusterTest := Test.CHClusterTest
	cleanup = func() {}

	pwd, err := os.Getwd()
	if err != nil {
		panic(fmt.Errorf("could not get working directory: %s", err.Error()))
	}

	for i, resource := range chClusterTest.Resources {
		freePort, err := freeport.GetFreePort()
		if err != nil {
			panic(fmt.Errorf("could not get free port for clickhouse resource:%d with error: %s", i, err.Error()))
		}
		resource.Port = strconv.Itoa(freePort)
	}

	var chSetupError error
	if chClusterTest.Network, err = pool.Client.CreateNetwork(dc.CreateNetworkOptions{
		Name: "clickhouse-network",
	}); err != nil {
		chSetupError = err
		log.Println(fmt.Errorf("could not create clickhouse cluster network: %s", err.Error()))
	}

	if chClusterTest.Zookeeper, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "zookeeper",
		Tag:        "3.5",
		Hostname:   "clickhouse-zookeeper",
		Name:       "clickhouse-zookeeper",
	}); err != nil {
		chSetupError = err
		log.Println(fmt.Errorf("could not create clickhouse cluster zookeeper: %s", err.Error()))
	}

	for i, chResource := range chClusterTest.Resources {
		if chResource.Resource, err = pool.RunWithOptions(&dockertest.RunOptions{
			Repository: "yandex/clickhouse-server",
			Tag:        "21-alpine",
			Hostname:   chResource.HostName,
			Name:       chResource.Name,
			PortBindings: map[dc.Port][]dc.PortBinding{
				"9000": {{HostIP: "127.0.0.1", HostPort: chResource.Port}},
			},
			ExposedPorts: []string{chResource.Port},
			Mounts:       []string{fmt.Sprintf(`%s/testdata/warehouse/clickhouse/cluster/%s:/etc/clickhouse-server`, pwd, chResource.Name)},
			Links:        []string{"clickhouse-zookeeper"},
		}); err != nil {
			chSetupError = err
			log.Println(fmt.Errorf("could not create clickhouse cluster %d: %s", i, err.Error()))
		}
	}

	if chClusterTest.Network != nil {
		if chClusterTest.Zookeeper != nil {
			if err = pool.Client.ConnectNetwork(chClusterTest.Network.ID, dc.NetworkConnectionOptions{
				Container:      chClusterTest.Zookeeper.Container.Name,
				EndpointConfig: &dc.EndpointConfig{},
			}); err != nil {
				chSetupError = err
				log.Println(fmt.Errorf("could not configure clickhouse clutser zookeeper network: %s", err.Error()))
			}
		}

		for i, chResource := range chClusterTest.Resources {
			if chResource.Resource != nil {
				if err = pool.Client.ConnectNetwork(chClusterTest.Network.ID, dc.NetworkConnectionOptions{
					Container: chResource.Resource.Container.Name,
					EndpointConfig: &dc.EndpointConfig{
						IPAddress: chResource.IPAddress,
					},
				}); err != nil {
					chSetupError = err
					log.Println(fmt.Errorf("could not configure clickhouse cluster %d network: %s", i, err.Error()))
				}
			}
		}
	}

	purgeResources := func() {
		if chClusterTest.Zookeeper != nil {
			log.Println("Purging clickhouse cluster zookeeper resource")
			if err := pool.Purge(chClusterTest.Zookeeper); err != nil {
				log.Println(fmt.Errorf("could not purge clickhouse cluster zookeeper resource: %s", err.Error()))
			}
		}
		for i, chResource := range chClusterTest.Resources {
			if chResource.Resource != nil {
				log.Printf("Purging clickhouse cluster %d resource \n", i)
				if err := pool.Purge(chResource.Resource); err != nil {
					log.Println(fmt.Errorf("could not purge clickhouse cluster %d resource: %s", i, err.Error()))
				}
			}
		}
		if chClusterTest.Network != nil {
			log.Println("Purging clickhouse cluster network resource")
			if err := pool.Client.RemoveNetwork(chClusterTest.Network.ID); err != nil {
				log.Println(fmt.Errorf("could not purge clickhouse cluster network resource: %s", err.Error()))
			}
		}
	}

	if chSetupError != nil {
		defer purgeResources()
		panic(fmt.Errorf("could not create WareHouse ClickHouse Cluster: %s", chSetupError.Error()))
	}

	for i, chResource := range chClusterTest.Resources {
		// Getting at which port the container is running
		chResource.Credentials.Port = chResource.Resource.GetPort("9000/tcp")

		// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
		if err := pool.Retry(func() error {
			var err error
			chResource.DB, err = clickhouse.Connect(*chResource.Credentials, true)
			if err != nil {
				return err
			}
			return chResource.DB.Ping()
		}); err != nil {
			chSetupError = fmt.Errorf("could not connect to warehouse clickhouse cluster: %d with error: %s", i, err.Error())
			log.Println(chSetupError)
		}
	}

	if chSetupError != nil {
		defer purgeResources()
		panic(fmt.Errorf("could not connect to warehouse clickhouse cluster: %s", chSetupError.Error()))
	}

	cleanup = purgeResources
	return
}

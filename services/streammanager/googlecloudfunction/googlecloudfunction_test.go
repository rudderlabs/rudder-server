package cloudfunctions

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/ory/dockertest/v3"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_logger "github.com/rudderlabs/rudder-server/mocks/utils/logger"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/testhelper"
)

var (
	hold       bool
	testConfig TestConfig
)

const (
	credentials            = "credentials"
	functionEnvironment    = "functionEnvironment"
	requireAuthentication  = "requireAuthentication"
	googleCloudFunctionUrl = "googleCloudFunctionUrl"
	functionName           = "functionName"
)

func Test_Timeout(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockLogger := mock_logger.NewMockLogger(mockCtrl)
	mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any()).AnyTimes()
	pkgLogger = mockLogger

	config := map[string]interface{}{
		"Credentials":            credentials,
		"FunctionEnvironment":    functionEnvironment,
		"RequireAuthentication":  requireAuthentication,
		"GoogleCloudFunctionUrl": googleCloudFunctionUrl,
		"FunctionName":           functionName,
		"TestConfig":             testConfig,
	}
	destination := backendconfig.DestinationT{Config: config}
	producer, err := NewProducer(&destination, common.Opts{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf(" %+v", err)
	}
	producer.opts = common.Opts{Timeout: 1 * time.Microsecond}
	json := `{
		"key1": "value1",
		"key2": "value2",
	}`
	statusCode, respStatus, responseMessage := producer.Produce([]byte(json), nil)
	const expectedStatusCode = 504
	if statusCode != expectedStatusCode {
		t.Errorf("Expected status code %d, got %d.", expectedStatusCode, statusCode)
	}

	const expectedRespStatus = "Failure"
	if respStatus != expectedRespStatus {
		t.Errorf("Expected response status %s, got %s.", expectedRespStatus, respStatus)
	}

	const expectedResponseMessage = "[GoogleCloudFunction] error :: Failed to insert Payload :: context deadline exceeded"
	if responseMessage != expectedResponseMessage {
		t.Errorf("Expected response message %s, got %s.", expectedResponseMessage, responseMessage)
	}
}

func TestMain(m *testing.M) {
	flag.BoolVar(&hold, "hold", false, "hold environment clean-up after test execution until Ctrl+C is provided")
	flag.Parse()

	// hack to make defer work, without being affected by the os.Exit in TestMain
	os.Exit(run(m))
}

func run(m *testing.M) int {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	pool.MaxWait = 2 * time.Minute
	if err != nil {
		log.Printf("Could not connect to docker: %s", err)
		return -1
	}
	cleanup := &testhelper.Cleanup{}
	defer cleanup.Run()
	config, err := SetupTestGoogleCloudFunction(pool, cleanup)
	if err != nil {
		log.Printf("Could not start google cloud function service: %s", err)
		return -1
	}
	testConfig = *config
	code := m.Run()
	blockOnHold()

	return code
}

func blockOnHold() {
	if !hold {
		return
	}

	fmt.Println("Test on hold, before cleanup")
	fmt.Println("Press Ctrl+C to exit")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
}

type cleaner interface {
	Cleanup(func())
	Log(...interface{})
}

func SetupTestGoogleCloudFunction(pool *dockertest.Pool, cln cleaner) (*TestConfig, error) {
	var config TestConfig
	// dockerContainer, err := pool.Run("googlecloudfunction/simulator-google-cloud-function", "latest", []string{})
	dir, er := os.Getwd()
	if er != nil {
		cln.Log(fmt.Errorf("could not find the folder: %v", er))
	}
	fullMountStr := fmt.Sprintf("%v/services/streammanager/googlecloudfunction/functions/helloWorld:/root/functions/helloWorld", dir)
	cln.Log(fullMountStr)
	dockerContainer, err := pool.RunWithOptions(&dockertest.RunOptions{
		// Repository: "googlecloudfunction/simulator-google-cloud-function",
		Repository: "theunit/google-cloud-functions-emulator",
		Tag:        "1.0.0",
		Mounts: []string{
			fullMountStr,
		},
		ExposedPorts: []string{"8010"},
		// Cmd: []string{"/sbin/my_init"},
	})
	cln.Log("After container intialization")
	if err != nil {
		return nil, fmt.Errorf("Could not start resource: %s", err)
	}
	cln.Log("Not returned due to error")
	cln.Cleanup(func() {
		if err := pool.Purge(dockerContainer); err != nil {
			cln.Log(fmt.Errorf("could not purge resource: %v", err))
		}
	})
	cln.Log("AFter cleanup")
	// dockerContainer.Container.Volumes = map[string]string{
	// 	fmt.Sprintf("%v/services/streammanager/googlecloudfunction/functions/helloWorld", dir): "/root/functions/helloWorld",
	// }
	exitCode, er := dockerContainer.Exec([]string{"ls"}, dockertest.ExecOptions{})
	if er != nil {
		cln.Log(fmt.Errorf("could not complete execution of deploy.sh: %w", er))
	}
	cln.Log("After execution exitCode:", exitCode)
	// config.Credentials = "{\n  \"type\": \"service_account\",\n  \"project_id\": \"big-query-integration-poc\",\n  \"private_key_id\": \"1fa764224a8015341b90fb19c48b97affeb0f07d\",\n  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCfIlr6wGzsR/zP\\n23q8vbi2KzdYE9vj7/bKS37+omdaN9HKmK8QGYpwDznIixA4+JhQn1RoE4R8TU7K\\nN/iEERXhRSiFIw1Y4QtgNs9iNQVOg+sRwfppJMzRDEepwu4kYfl/SnrNQ3b7iHrf\\n6keJFbogxN2m/dHcPdmomnIpB35qkJsvDor/opD/d6UmqhZCxQKqYTLaV1n2UiF4\\nwUQ/rziU2L1yOoG3eYOgP5eh+7lhb4t7X0zVxyI++rXzSE1bewUyRgDqzkCivFfk\\nry2Mvq1HfYVvhD+X3+S/NB5arzVuR3H86qkzLYTkXhrlka4fmPtMljz3DkZ77rKB\\nmq/rba+5AgMBAAECggEABeaCNRX5c/FfYF2k+WaXeLm1faCO4K6/GdUeylY/Ossj\\nDj4HD7PCvFD0NYlieppNG4As4wcGPI2pRDo7DrqLcyTbUcRw9ECz6Ude/Sc2ISkn\\nCCuHG2rv5ThtV9AQgGzZkWyzjPTZbo4q8C6BpIWXtytVhKGYrG/mqCfB39+VQmI3\\nQRKqPp9ntse7bIdkUReyFYeWikYzDP7bbQ/PGtYwJf3fp+aq+a8Ss11+WGvAt7y5\\n3bXqsQqkl2YwZx1husTo93DTa3hYPCn4pB8Y1yahOZoe3TUImw9/iMov9Efu2gFQ\\n5VOa7uU88HRZgppwGNCAq1FVx7awruGcv9Dr13re4QKBgQDVcMPEHSd4czDnw+ob\\n4BkzyPchY3J0PYemx6NpOWwdvazJls3mE0HQCVSxMU/KT3BHjP2LUwu4QE2ylOJD\\nCBL5Pes+qcR5GjPxqjXp3b+OLkCn8NqB1T5Mj8oVUYrG2nkAUiBNFoD7IhubdYjE\\nEhH0SZy3SIcvYmme2PzFDjw9hQKBgQC+3XvxGYKlRpfWbnaQxh655GYOvWaMeFOZ\\nrFnjLHkmnumejCrHAi87p8DKbRRSJyAtIS7vV5vZB3Fn8h8oBL+Dzj1hgwMRb3AV\\nhUy0l6orgWRteTIVZsFY8etTkvOkGgEgsNkU18HWEIEUTUZdYmBQJK+pY+P3ijaG\\nVuZvJB+1pQKBgQCIIKFaxNVVpvH7/yGioayxMG0daFWo+U04+36nL65a0YwQDhRH\\ntuR0kF7pm++tsjiECdHzOAXzf1+OlBIyekXPnMQadSAPtLyIVuyHuZvgTHOEKMLT\\np3NSVSqnqhf+d3xQzhA32jBs+vXskmul0XiN+xzucF13xSZj3zRnYYreGQKBgQCT\\nhWDsoskFsaBeDpb5owfQqiXvsph2bfI/zqTTH2asYRs0fiX6F0gmlaw/STvxm2gV\\nOZarS02b21LeApfuOG1GFLwAiGBws28wUw1McAUJB0N2EbySKg+7DZDT0bsa1TaV\\n69p9b53UNIFwDaIbP5QtnY1gIWLryxoFj1kgAg24OQKBgGW3cjZIbm2RAnMJYkwj\\nquuxVKSr1izkpgwN/V8LFlVgWav0bYbRWVJgttP17mZEAfXhOsTMhwTofcyNqwq4\\n1ez8ZI6KJ9TNr7YXDZHS0CL14F4Pxx9PLHkU2T7vn9X94zzRVdaS4fm48NUgOi1c\\nsyBgg2CeNFUZhQeqK9h8mj+2\\n-----END PRIVATE KEY-----\\n\",\n  \"client_email\": \"san-bqstream@big-query-integration-poc.iam.gserviceaccount.com\",\n  \"client_id\": \"105449441927191315021\",\n  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/san-bqstream%40big-query-integration-poc.iam.gserviceaccount.com\",\n  \"universe_domain\": \"googleapis.com\"\n}\n"

	// cloudFunctionService, err := cloudfunctions.NewService(context.Background(), option.WithoutAuthentication(), option.WithEndpoint(fmt.Sprintf("127.0.0.1:%s", dockerContainer.GetPort("8010/tcp"))))
	if err == nil {
		cln.Log("Service Created")
	}

	if err := pool.Retry(func() error {
		_, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		// _, err = cloudFunctionService.Operations.Get("projects/project/locations/us-central1/functions/helloWorld").Context(ctx).Do()
		return err
	}); err != nil {
		return nil, fmt.Errorf("Could not connect to Google cloud function service")
	}
	return &config, nil
}

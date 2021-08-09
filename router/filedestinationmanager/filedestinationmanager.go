package filedestinationmanager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"sync"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	FILE = "file"
)

var (
	FileUploadDestinations []string
	pkgLogger              logger.LoggerI
	customManagerMap       map[string]*CustomManagerT
	disableEgress          bool
)

type CustomManagerT struct {
	destType             string
	managerType          string
	destinationsMap      map[string]*CustomDestination
	destinationLockMap   map[string]*sync.RWMutex
	latestConfig         map[string]backendconfig.DestinationT
	configSubscriberLock sync.RWMutex
}

type CustomDestination struct {
	Config interface{}
	Client interface{}
}

type DestinationManager interface {
	UploadFile(jsonData json.RawMessage, sourceID string, destID string) (int, string)
}

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("router").Child("customdestinationmanager")
}

func loadConfig() {
	FileUploadDestinations = []string{"MARKETO_BULK"}
	config.RegisterBoolConfigVariable(false, &disableEgress, false, "disableEgress")
}

func UploadFile() {

	url := "https://585-AXP-425.mktorest.com/bulk/v1/leads.json"
	method := "POST"

	payload := &bytes.Buffer{}
	writer := multipart.NewWriter(payload)
	_ = writer.WriteField("format", "csv")
	_ = writer.WriteField("file", "@lead_data.csv")
	_ = writer.WriteField("access_token", "d1d4698b-4c11-4da9-9744-b09d4b53cb16:ab")
	err := writer.Close()
	if err != nil {
		fmt.Println(err)
		return
	}

	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		fmt.Println(err)
		return
	}
	req.Header.Add("Authorization", "Bearer d1d4698b-4c11-4da9-9744-b09d4b53cb16:ab")

	req.Header.Set("Content-Type", writer.FormDataContentType())
	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(body))
}

func New(destType string) DestinationManager {
	if misc.ContainsString(FileUploadDestinations, destType) {

		managerType := FILE
		customManager, ok := customManagerMap[destType]
		if ok {
			return customManager
		}

		customManager = &CustomManagerT{
			destType:           destType,
			managerType:        managerType,
			destinationsMap:    make(map[string]*CustomDestination),
			destinationLockMap: make(map[string]*sync.RWMutex),
			latestConfig:       make(map[string]backendconfig.DestinationT),
		}
		rruntime.Go(func() {
			customManager.backendConfigSubscriber()
		})
		return customManager
	}

	return nil
}

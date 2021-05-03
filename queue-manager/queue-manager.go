package queuemanager

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/tidwall/gjson"
)

var (
	QueueManager QueueManagerI
	webPort      int
	pkgLogger    logger.LoggerI
)

var Eb utils.PublishSubscriber = new(utils.EventBus)

type QueueManagerI interface {
	Subscribe(channel chan utils.DataEvent)
}
type QueueManagerT struct {
}

type clearQueueRequestPayload struct {
	SourceID      string `json:"source_id"`
	DestinationID string `json:"destination_id"`
}

func loadConfig() {
	//Port where GW is running
	webPort = config.GetInt("Gateway.webPort", 8080)
}

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("queuemanager")
}

/*
Subscribe subscribes a channel to a specific topic
*/
func (qm *QueueManagerT) Subscribe(channel chan utils.DataEvent) {
	Eb.Subscribe("queuemanager", channel)
}

// Setup backend config
func Setup() {
	pkgLogger.Infof("setting up queuemanager. starting http handler on port: %v", webPort)
	QueueManager = new(QueueManagerT)

	/*srvMux := mux.NewRouter()
	srvMux.HandleFunc("/", healthHandler)
	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(webPort),
		Handler:           bugsnag.Handler(srvMux),
		ReadTimeout:       config.GetDuration("ReadTimeOutInSec", 0*time.Second),
		ReadHeaderTimeout: config.GetDuration("ReadHeaderTimeoutInSec", 0*time.Second),
		WriteTimeout:      config.GetDuration("WriteTimeOutInSec", 10*time.Second),
		IdleTimeout:       config.GetDuration("IdleTimeoutInSec", 720*time.Second),
		MaxHeaderBytes:    config.GetInt("MaxHeaderBytes", 524288),
	}
	pkgLogger.Fatal(srv.ListenAndServe())*/

}

func ClearHandler(w http.ResponseWriter, r *http.Request) {
	pkgLogger.LogRequest(r)
	var errorMessage string
	defer func() {
		if errorMessage != "" {
			pkgLogger.Debug(errorMessage)
			http.Error(w, response.GetStatus(errorMessage), 400)
		}
	}()

	payload, _, err := getPayloadAndWriteKey(w, r)
	if err != nil {
		errorMessage = err.Error()
		return
	}

	if !gjson.ValidBytes(payload) {
		errorMessage = response.GetStatus(response.InvalidJSON)
		return
	}

	var reqPayload clearQueueRequestPayload
	err = json.Unmarshal(payload, &reqPayload)
	if err != nil {
		errorMessage = err.Error()
		return
	}

	if reqPayload.SourceID == "" {
		errorMessage = "Empty source id"
		return
	}

	w.Write([]byte(fmt.Sprintf("{ \"op_id\": %d }", 1)))
}

func getPayloadAndWriteKey(w http.ResponseWriter, r *http.Request) ([]byte, string, error) {
	var err error
	writeKey, _, ok := r.BasicAuth()
	if !ok || writeKey == "" {
		err = errors.New(response.NoWriteKeyInBasicAuth)
		return []byte{}, "", err
	}
	payload, err := getPayloadFromRequest(r)
	if err != nil {
		return []byte{}, writeKey, err
	}
	return payload, writeKey, err
}

func getPayloadFromRequest(r *http.Request) ([]byte, error) {
	if r.Body != nil {
		payload, err := ioutil.ReadAll(r.Body)
		r.Body.Close()
		return payload, err
	}
	return []byte{}, errors.New(response.RequestBodyNil)
}

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"net/http"

	"github.com/tidwall/sjson"

	"github.com/go-pg/pg"

	"github.com/go-pg/pg/orm"
)

//43390
// export api
// curl -u c44ba00c78400bd1dfac1583995af31a:70f0beb2806c41f36ca347f818d1b37b 'https://amplitude.com/api/2/export?start=20190808T00&end=20190809T23' >> our.zip

var (
	out                        *os.File
	batchSize                  int
	rudderSend                 bool
	userIDToChannel            map[string]chan []byte
	client                     *http.Client
	noOfUsers                  int
	noOfBatches                int
	db                         *pg.DB
	compare                    bool
	writeKey                   = "1P6q8fcXrkmekovCdk0a3gFq30X"
	lock                       sync.Mutex
	netlock                    sync.Mutex
	matchedInsertInOrderNo     int64
	matchedOtherPropsInOrderNo int64
	userSendCompleteCount      int64
	//userIDToJob
)

type userEventList struct {
	UserID    string   `sql:",pk"`
	InsertIds []string `pg:",array"`
	Events    []int    `pg:",array"`
}

func (userEvent *userEventList) String() {
	fmt.Printf("UserId: %v, InsertIds: %v, Events: %v \n", userEvent.UserID, userEvent.InsertIds, userEvent.Events)
}

func mainFunc(inputJSONFile string, outPutJSONFile string, outPutNoUserFile string) {
	compare = true
	batchSize = 10
	rudderSend = false
	noOfUsers = 20000
	noOfBatches = 1000
	userIDToChannel = make(map[string]chan []byte)
	client = &http.Client{}

	db = pg.Connect(&pg.Options{
		User:     "ubuntu",
		Password: "ubuntu",
		Database: "user", //"testuserevent", //"userevent",
		Addr:     "localhost:5432",
	})

	if db != nil {
		fmt.Println("CONNECTED TO DB")
		defer db.Close()
	} else {
		fmt.Println("CONNECTION ERROR")
		return
	}

	if !compare {
		opts := &orm.CreateTableOptions{
			IfNotExists: true,
		}

		crErr := db.CreateTable(&userEventList{}, opts)

		if crErr != nil {
			panic(crErr)
		}
	}

	/* fileOptions, errorTracking := ioutil.ReadDir(inputJSONDir)

	if errorTracking != nil {
		panic(errorTracking)
	}

	for _, fo := range fileOptions {
		if !fo.IsDir() {
			fmt.Println(fo.Name())
		}
	}

	return */

	f, err := os.Open(inputJSONFile)
	if err != nil {
		fmt.Println("error openinf file")
		return
	}
	reader := bufio.NewReader(f)

	var errCreate error

	out, errCreate = os.Create(outPutJSONFile)
	if errCreate != nil {
		fmt.Println("error openinf file")
		return
	}

	defer out.Close()

	outNoUser, errCreateFile := os.Create(outPutNoUserFile)
	if errCreateFile != nil {
		fmt.Println("error openinf file")
		return
	}

	defer outNoUser.Close()

	done := make(chan bool)
	doneWorker := make(chan bool)

	var unmarhsalledData map[string]interface{}
	// outputJSON := []byte(`{"events":[]}`)
	//outputRudderJSON := []byte(`{"batch" : [{"rl_message": {}}], "sent_at":""}`)
	// eventTypeMap := make(map[string]bool)
	unmarhsalledData = nil
	// rudderEventMap := make(map[string]interface{})
	// rudderUserMap := make(map[string]interface{})

	b, err := reader.ReadBytes('\n')

	proccessed := 0
	nonUser := 0

	for err != io.EOF {
		proccessed++
		unmarhsalledData = nil
		json.Unmarshal(b, &unmarhsalledData)

		if unmarhsalledData["user_id"] == nil || unmarhsalledData["$insert_id"] == nil || unmarhsalledData["event_type"] == nil {
			outNoUser.Write(b)
			//fmt.Println(string(b))
			nonUser++
			fmt.Println("===", nonUser)
			b, err = reader.ReadBytes('\n')
			continue
		}

		_, found := userIDToChannel[unmarhsalledData["user_id"].(string)]

		if !found {
			if len(userIDToChannel) >= noOfUsers {
				// fmt.Println("user limit already reached")
				b, err = reader.ReadBytes('\n')
				continue
			}
			userIDToChannel[unmarhsalledData["user_id"].(string)] = make(chan []byte)
			go transform(userIDToChannel[unmarhsalledData["user_id"].(string)], unmarhsalledData["user_id"].(string), done, doneWorker)
		}

		//fmt.Println(string(b))
		userIDToChannel[unmarhsalledData["user_id"].(string)] <- b

		// user query
		/* if unmarhsalledData["user_id"] != nil {
			_, ok := eventTypeMap[unmarhsalledData["user_id"].(string)]
			if !ok {
				count++
				eventTypeMap[unmarhsalledData["user_id"].(string)] = true
			}
		} */

		b, err = reader.ReadBytes('\n')
	}

	fmt.Println("Processed: ", proccessed)
	fmt.Println("nonUser: ", nonUser)
	fmt.Println("allUsers: ", len(userIDToChannel))

	for _ = range userIDToChannel {
		done <- true
	}

	for _ = range userIDToChannel {
		<-doneWorker
	}

	if compare {
		fmt.Println(" matchedInsertInOrderNo: ", matchedInsertInOrderNo)
		fmt.Println(" matchedOtherPropsInOrderNo: ", matchedOtherPropsInOrderNo)
	}

	fmt.Println("====done=====")

	//fmt.Println("users: ", count)

	//fmt.Println(string(outputJSON))
}

func transform(requestQ chan []byte, userID string, done chan bool, doneWorker chan bool) {

	var eventList []int
	var insertIDList []string

	outputJSON := []byte(`{"events":[]}`)
	outputRudderJSON := []byte(`{"batch" : [{"rl_message": {}}], "sent_at":""}`)
	eventTypeMap := make(map[string]bool)
	// unmarhsalledData = nil
	rudderEventMap := make(map[string]interface{})
	rudderUserMap := make(map[string]interface{})

	var count int //initialised to 0
	var batchNumber int
	var unmarhsalledData map[string]interface{}

	for {

		select {
		case jsonByte := <-requestQ:

			//fmt.Println("===data===", string(jsonByte))

			if compare {
				json.Unmarshal(jsonByte, &unmarhsalledData)
				insertID := unmarhsalledData["$insert_id"]
				mappedEventProp, _ := unmarhsalledData["event_properties"].(map[string]interface{})
				mappedUserPro, _ := unmarhsalledData["user_properties"].(map[string]interface{})

				eb, _ := json.Marshal(mappedEventProp)
				ub, _ := json.Marshal(mappedUserPro)

				eb = append(eb, ub...)

				sum := 0
				for b := range eb {
					sum += b
				}

				//fmt.Println("==user props===", mappedUserPro)
				//fmt.Println("===event props===", mappedEventProp)

				//fmt.Println("===sum===", sum)
				insertIDList = append(insertIDList, insertID.(string))
				eventList = append(eventList, sum)
				break
			}

			if batchNumber+1 > noOfBatches {
				break
			}
			json.Unmarshal(jsonByte, &unmarhsalledData)
			rudderEventMap = make(map[string]interface{})
			rudderUserMap = make(map[string]interface{})

			if unmarhsalledData["event_type"] != nil {
				_, ok := eventTypeMap[unmarhsalledData["event_type"].(string)]
				// skipping the check for unique event_types
				if ok || !ok {
					eventTypeMap[unmarhsalledData["event_type"].(string)] = true
					//fmt.Println(unmarhsalledData["event_type"])
					outputJSON, _ = sjson.SetBytes(outputJSON, "events."+strconv.Itoa(count)+".event_type", unmarhsalledData["event_type"].(string))
					outputRudderJSON, _ = sjson.SetBytes(outputRudderJSON, "batch."+strconv.Itoa(count)+".rl_message.rl_event", unmarhsalledData["event_type"])

					mappedEventProp, _ := unmarhsalledData["event_properties"].(map[string]interface{})
					mappedUserPro, _ := unmarhsalledData["user_properties"].(map[string]interface{})

					insertID := unmarhsalledData["$insert_id"]

					eb, _ := json.Marshal(mappedEventProp)
					ub, _ := json.Marshal(mappedUserPro)

					eb = append(eb, ub...)

					sum := 0
					for b := range eb {
						sum += b
					}

					insertIDList = append(insertIDList, insertID.(string))
					eventList = append(eventList, sum)

					for ke, ve := range mappedEventProp {
						if strings.Contains(ke, "$") {
							rudderEventMap[ke[1:]] = ve
						} else {
							rudderEventMap[ke] = ve
						}
					}

					for ku, vu := range mappedUserPro {
						switch ku {
						/* case "email":
							rudderUserMap["rl_email"] = vu
							break
						case "first_name":
							rudderUserMap["rl_firstname"] = vu
							break
						case "last_name":
							rudderUserMap["rl_lastname"] = vu
							break
						case "gender":
							rudderUserMap["rl_gender"] = vu
							break
						case "title":
							rudderUserMap["rl_title"] = vu
							break */
						default:
							rudderUserMap[ku] = vu
							break
						}
					}

					//fmt.Println(rudderEventMap)
					//fmt.Println(rudderUserMap)

					//eventProps, _ := json.Marshal(mappedEventProp)
					outputJSON, _ = sjson.SetBytes(outputJSON, "events."+strconv.Itoa(count)+".event_properties", rudderEventMap)
					outputJSON, _ = sjson.SetBytes(outputJSON, "events."+strconv.Itoa(count)+".user_properties", rudderUserMap)

					outputRudderJSON, _ = sjson.SetBytes(outputRudderJSON, "batch."+strconv.Itoa(count)+".rl_message.rl_properties", rudderEventMap)
					outputRudderJSON, _ = sjson.SetBytes(outputRudderJSON, "batch."+strconv.Itoa(count)+".rl_message.rl_user_properties", rudderUserMap)
					outputRudderJSON, _ = sjson.SetBytes(outputRudderJSON, "batch."+strconv.Itoa(count)+".rl_message.rl_timestamp", time.Now())

					addOtherTransformation(&outputRudderJSON, count, unmarhsalledData)

					//fmt.Println(string(outputRudderJSON))

					count++
				}

				//fmt.Println("event_type", unmarhsalledData["event_type"])
			} else {
				outputJSON, _ = sjson.SetBytes(outputJSON, "events."+strconv.Itoa(count)+".event_type", unmarhsalledData["event_type"])
				outputRudderJSON, _ = sjson.SetBytes(outputRudderJSON, "batch."+strconv.Itoa(count)+".rl_message.rl_event", unmarhsalledData["event_type"])

				mappedEventProp, _ := unmarhsalledData["event_properties"].(map[string]interface{})

				mappedUserPro, _ := unmarhsalledData["user_properties"].(map[string]interface{})

				insertID := unmarhsalledData["$insert_id"]

				eb, _ := json.Marshal(mappedEventProp)
				ub, _ := json.Marshal(mappedUserPro)

				eb = append(eb, ub...)

				sum := 0
				for b := range eb {
					sum += b
				}

				insertIDList = append(insertIDList, insertID.(string))
				eventList = append(eventList, sum)

				//eventProps, _ := json.Marshal(mappedEventProp)
				outputJSON, _ = sjson.SetBytes(outputJSON, "events."+strconv.Itoa(count)+".event_properties", mappedEventProp)
				outputJSON, _ = sjson.SetBytes(outputJSON, "events."+strconv.Itoa(count)+".user_properties", mappedUserPro)

				outputRudderJSON, _ = sjson.SetBytes(outputRudderJSON, "batch."+strconv.Itoa(count)+".rl_message.rl_properties", mappedEventProp)
				outputRudderJSON, _ = sjson.SetBytes(outputRudderJSON, "batch."+strconv.Itoa(count)+".rl_message.rl_user_properties", mappedUserPro)
				outputRudderJSON, _ = sjson.SetBytes(outputRudderJSON, "batch."+strconv.Itoa(count)+".rl_message.rl_timestamp", time.Now())

				addOtherTransformation(&outputRudderJSON, count, unmarhsalledData)

				count++
			}

			if count%batchSize == 0 {
				//fmt.Println("========", count)
				outputRudderJSON, _ = sjson.SetBytes(outputRudderJSON, "sent_at", time.Now())
				outputRudderJSON, _ = sjson.SetBytes(outputRudderJSON, "writeKey", writeKey)

				if rudderSend {
					// sleep to check if we still hit EOF
					time.Sleep(10 * time.Millisecond)
					sendToRudder(outputRudderJSON)
				}

				writeToOutputFile(outputRudderJSON)

				outputJSON = []byte(`{"events":[]}`)
				outputRudderJSON = []byte(`{"batch" : [{"rl_message": {}}], "sent_at":""}`)
				eventTypeMap = make(map[string]bool)
				// unmarhsalledData = nil
				rudderEventMap = make(map[string]interface{})
				rudderUserMap = make(map[string]interface{})
				count = 0
				batchNumber++

			}
		case <-done:
			if compare {
				// fmt.Println("==userId==", unmarhsalledData["user_id"], userID)
				lock.Lock()
				testUser := userEventList{UserID: userID}
				selectErr := db.Select(&testUser)
				lock.Unlock()
				if selectErr == nil {
					// testUser.String()
					// fmt.Println("==current user eventlist from AM==", eventList)
					if len(testUser.Events) != len(eventList) {
						fmt.Printf("userId %v event length don't match!!! \"stored events:\" %v \"AM downloaded events:\" %v\n", userID, len(testUser.Events), len(eventList))
						doneWorker <- true
						return
					}
					countSuccess := 0
					for insertIndex := range testUser.InsertIds {
						if testUser.InsertIds[insertIndex] == insertIDList[insertIndex] {
							countSuccess++
						} else {
							fmt.Printf("stored insertId %v, AM insertId %v: \n", testUser.InsertIds[insertIndex], insertIDList[insertIndex])
						}
					}
					if countSuccess == len(testUser.InsertIds) {
						fmt.Println("insert order is right!!!")
						atomic.AddInt64(&matchedInsertInOrderNo, 1)
					}
					isMatch := true
					for index := range testUser.Events {
						if testUser.Events[index] != eventList[index] {
							fmt.Printf("userId %v event %v don't match \n", userID, index+1)
							isMatch = false
						}
					}
					if isMatch {
						atomic.AddInt64(&matchedOtherPropsInOrderNo, 1)
					}
				} else {
					//panic(selectErr)
					fmt.Printf("error fetching stored eventList for user %v, err: %v \n", userID, selectErr)
				}

				fmt.Println("done with comparing")
				doneWorker <- true
				return
			}

			if count > 0 {
				//fmt.Println("flushing remaining batch")
				//fmt.Println("==userId==", userID)
				outputRudderJSON, _ = sjson.SetBytes(outputRudderJSON, "sent_at", time.Now())
				outputRudderJSON, _ = sjson.SetBytes(outputRudderJSON, "writeKey", writeKey)

				if rudderSend {
					// sleep to check if we still hit EOF
					time.Sleep(10 * time.Millisecond)
					sendToRudder(outputRudderJSON)
				}

				writeToOutputFile(outputRudderJSON)

				outputJSON = []byte(`{"events":[]}`)
				outputRudderJSON = []byte(`{"batch" : [{"rl_message": {}}], "sent_at":""}`)
				eventTypeMap = make(map[string]bool)
				// unmarhsalledData = nil
				rudderEventMap = make(map[string]interface{})
				rudderUserMap = make(map[string]interface{})
				count = 0
				batchNumber++
			}

			fmt.Println("===event and user list===", eventList, userID)
			lock.Lock()
			userEvent := userEventList{UserID: userID, Events: eventList, InsertIds: insertIDList}
			if db != nil {
				fmt.Println("db conn present")
				insertErr := db.Insert(&userEvent)
				if insertErr != nil {
					panic(insertErr)
				}
			}

			// test purpose
			testUser := userEventList{UserID: userID}
			selectErr := db.Select(&testUser)
			if selectErr == nil {
				testUser.String()
			} else {
				panic(selectErr)
			}

			lock.Unlock()

			atomic.AddInt64(&userSendCompleteCount, 1)

			fmt.Println("===done user===", userSendCompleteCount)
			doneWorker <- true
			return
		}
	}

	// outputRudderJSON, _ = sjson.SetBytes(outputRudderJSON, "sent_at", time.Now())

}

func writeToOutputFile(outputRudderJSON []byte) {
	var outNeeded map[string]interface{}
	//_ = json.Unmarshal(outputJSON, &outNeeded)

	//fmt.Println(string(outputRudderJSON))

	_ = json.Unmarshal(outputRudderJSON, &outNeeded)

	//fmt.Println(outNeeded)

	printPretty, _ := json.Marshal(outNeeded)

	//fmt.Print(string(printPretty))

	printPretty = append(printPretty, "\n"...)

	w := bufio.NewWriter(out)
	//_, errorWrite := w.WriteString(string(printPretty))
	_, errorWrite := w.Write(printPretty)

	if errorWrite != nil {
		fmt.Println(errorWrite)
	}

	w.Flush()
}

func addOtherTransformation(outputJSON *[]byte, count int, unmarhsalledSourceData map[string]interface{}) {
	// For torpedo it's always track for now
	*outputJSON, _ = sjson.SetBytes(*outputJSON, "batch."+strconv.Itoa(count)+".rl_message.rl_type", "track")

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "batch."+strconv.Itoa(count)+".rl_message.rl_context.rl_traits.rl_address.rl_city", unmarhsalledSourceData["city"])

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "batch."+strconv.Itoa(count)+".rl_message.rl_context.rl_traits.rl_address.rl_country", unmarhsalledSourceData["country"])

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "batch."+strconv.Itoa(count)+".rl_message.rl_context.rl_os.rl_name", unmarhsalledSourceData["os_name"])

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "batch."+strconv.Itoa(count)+".rl_message.rl_context.rl_os.rl_version", unmarhsalledSourceData["os_version"])

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "batch."+strconv.Itoa(count)+".rl_message.rl_context.rl_device.rl_id", unmarhsalledSourceData["device_id"])

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "batch."+strconv.Itoa(count)+".rl_message.rl_context.rl_device.rl_name", unmarhsalledSourceData["device_brand"])

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "batch."+strconv.Itoa(count)+".rl_message.rl_context.rl_device.rl_model", unmarhsalledSourceData["device_model"])

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "batch."+strconv.Itoa(count)+".rl_message.rl_context.rl_device.rl_manufacturer", unmarhsalledSourceData["device_manufacturer"])

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "batch."+strconv.Itoa(count)+".rl_message.rl_context.rl_network.rl_carrier", unmarhsalledSourceData["device_carrier"])

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "batch."+strconv.Itoa(count)+".rl_message.rl_integrations", []string{"amplitude"})

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "batch."+strconv.Itoa(count)+".rl_message.rl_context.rl_traits.rl_anonymous_id", unmarhsalledSourceData["user_id"])

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "batch."+strconv.Itoa(count)+".rl_message.rl_message_id", unmarhsalledSourceData["$insert_id"])

}

func sendToRudder(jsonPayload []byte) {
	netlock.Lock()
	defer netlock.Unlock()
	fmt.Println("sending to rudder...")
	time.Sleep(10 * time.Millisecond)
	req, err := http.NewRequest("POST", "http://localhost:8080/hello", bytes.NewBuffer(jsonPayload))
	req.Close = true

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "close")

	//client := &http.Client{}
	resp, err := client.Do(req)

	if resp != nil && resp.Body != nil {
		fmt.Println("response Status:", resp.Status)
		fmt.Println("response Headers:", resp.Header)
		body, _ := ioutil.ReadAll(resp.Body)
		fmt.Println("response Body:", string(body))
		defer resp.Body.Close()
	}

	if err != nil {
		panic(err)
	}

}

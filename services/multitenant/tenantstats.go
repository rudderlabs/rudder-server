package multitenant

import (
	"bytes"
	"encoding/gob"
	"math"
	"os"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var jobQueryBatchSize int
var RouterInMemoryJobCounts map[string]map[string]map[string]int
var routerJobCountMutex sync.RWMutex
var RouterInputRates map[string]map[string]map[string]misc.MovingAverage
var pkgLogger logger.LoggerI

func Init() {
	pkgLogger = logger.NewLogger().Child("services").Child("multitenant")
	RouterInMemoryJobCounts = make(map[string]map[string]map[string]int)
	RouterInMemoryJobCounts["router"] = make(map[string]map[string]int)
	RouterInMemoryJobCounts["batch_router"] = make(map[string]map[string]int)
	prePopulateRouterPileUpCounts()
	RouterInputRates = make(map[string]map[string]map[string]misc.MovingAverage)
	RouterInputRates["router"] = make(map[string]map[string]misc.MovingAverage)
	RouterInputRates["batch_router"] = make(map[string]map[string]misc.MovingAverage)
	config.RegisterIntConfigVariable(10000, &jobQueryBatchSize, true, 1, "Router.jobQueryBatchSize")
	go writerouterPileUpStatsEncodedToFile()
}

func writerouterPileUpStatsEncodedToFile() {
	for {
		file, _ := os.Create("router_pile_up_stat_persist.txt")
		buf := new(bytes.Buffer)
		encoder := gob.NewEncoder(buf)
		err := encoder.Encode(RouterInMemoryJobCounts)
		if err != nil {
			panic(err)
		}
		_, err = file.Write(buf.Bytes())
		if err != nil {
			panic(err)
		}
		time.Sleep(60 * time.Second)
	}
}

func prePopulateRouterPileUpCounts() {
	byteData, err := os.ReadFile("router_pile_up_stat_persist.txt")
	if err != nil {
		panic(err)
	}
	if len(byteData) == 0 {
		return
	}
	bufferedData := bytes.NewBuffer(byteData)
	decoder := gob.NewDecoder(bufferedData)
	err = decoder.Decode(&RouterInMemoryJobCounts)
	if err != nil {
		panic(err)
	}
}

func AddToInMemoryCount(customerID string, destinationType string, count int, tableType string) {
	routerJobCountMutex.RLock()
	_, ok := RouterInMemoryJobCounts[tableType][customerID]
	if !ok {
		routerJobCountMutex.RUnlock()
		routerJobCountMutex.Lock()
		RouterInMemoryJobCounts[tableType][customerID] = make(map[string]int)
		routerJobCountMutex.Unlock()
		routerJobCountMutex.RLock()
	}
	routerJobCountMutex.RUnlock()
	RouterInMemoryJobCounts[tableType][customerID][destinationType] += count
}

func RemoveFromInMemoryCount(customerID string, destinationType string, count int, tableType string) {
	routerJobCountMutex.RLock()
	_, ok := RouterInMemoryJobCounts[tableType][customerID]
	if !ok {
		routerJobCountMutex.RUnlock()
		routerJobCountMutex.Lock()
		RouterInMemoryJobCounts[tableType][customerID] = make(map[string]int)
		routerJobCountMutex.Unlock()
		routerJobCountMutex.RLock()
	}
	routerJobCountMutex.RUnlock()
	RouterInMemoryJobCounts[tableType][customerID][destinationType] += -1 * count
}

func ReportProcLoopAddStats(stats map[string]map[string]int, timeTaken time.Duration, tableType string) {
	for key := range stats {
		routerJobCountMutex.RLock()
		_, ok := RouterInputRates[tableType][key]
		if !ok {
			routerJobCountMutex.RUnlock()
			routerJobCountMutex.Lock()
			RouterInputRates[tableType][key] = make(map[string]misc.MovingAverage)
			routerJobCountMutex.Unlock()
			routerJobCountMutex.RLock()
		}
		routerJobCountMutex.RUnlock()
		for destType := range stats[key] {
			routerJobCountMutex.RLock()
			_, ok := RouterInputRates[tableType][key][destType]
			if !ok {
				routerJobCountMutex.RUnlock()
				routerJobCountMutex.Lock()
				RouterInputRates[tableType][key][destType] = misc.NewMovingAverage()
				routerJobCountMutex.Unlock()
				routerJobCountMutex.RLock()
			}
			routerJobCountMutex.RUnlock()
			RouterInputRates[tableType][key][destType].Add(float64(stats[key][destType]) * float64(time.Second) / float64(timeTaken))
			AddToInMemoryCount(key, destType, stats[key][destType], tableType)
		}
	}
	for customerKey := range RouterInputRates[tableType] {
		_, ok := stats[customerKey]
		if !ok {
			for destType := range stats[customerKey] {
				routerJobCountMutex.Lock()
				RouterInputRates[tableType][customerKey][destType].Add(0)
				routerJobCountMutex.Unlock()
			}
		}

		for destType := range RouterInputRates[tableType][customerKey] {
			_, ok := stats[customerKey][destType]
			if !ok {
				routerJobCountMutex.Lock()
				RouterInputRates[tableType][customerKey][destType].Add(0)
				routerJobCountMutex.Unlock()
			}
		}
	}
}

func GetRouterPickupJobs(destType string, earliestJobMap map[string]time.Time, sortedLatencyList []string, noOfWorkers int, routerTimeOut time.Duration, latencyMap map[string]misc.MovingAverage) map[string]int {
	customerPickUpCount := make(map[string]int)
	runningTimeCounter := float64(noOfWorkers) * float64(routerTimeOut/time.Second)
	routerJobCountMutex.RLock()
	defer routerJobCountMutex.RUnlock()
	runningJobCount := jobQueryBatchSize

	for _, customerKey := range sortedLatencyList {
		timeRequired := float64(latencyMap[customerKey].Value() * RouterInputRates["router"][customerKey][destType].Value() * float64(routerTimeOut/time.Second))
		///int(float64(jobQueryBatchSize)*(customerLiveCount[customerKey]/totalCount)) + 1
		customerPickUpCount[customerKey] = int(math.Min(timeRequired, runningTimeCounter) / latencyMap[customerKey].Value())
		if RouterInMemoryJobCounts["router"][customerKey][destType] > 0 {
			customerPickUpCount[customerKey] = customerPickUpCount[customerKey] + 1
		}
		if customerPickUpCount[customerKey] > RouterInMemoryJobCounts["router"][customerKey][destType] {
			customerPickUpCount[customerKey] = RouterInMemoryJobCounts["router"][customerKey][destType]
			timeRequired = latencyMap[customerKey].Value() * float64(RouterInMemoryJobCounts["router"][customerKey][destType]) * float64(routerTimeOut/time.Second)
		}
		//modify time required
		runningTimeCounter = runningTimeCounter - timeRequired
		runningJobCount = runningJobCount - customerPickUpCount[customerKey]
		//runningJobCount should be a number large enough to ensure fairness but small enough to not cause OOM Issues
		if runningJobCount <= 0 {
			pkgLogger.Infof(`[Router Pickup] Total Jobs picked up crosses the maxJobQueryBatchSize`)
			return customerPickUpCount
		}
	}

	if runningTimeCounter <= 0 {
		return customerPickUpCount
	}

	for _, customerKey := range sortedLatencyList {
		if RouterInMemoryJobCounts["router"][customerKey][destType] == 0 {
			continue
		}
		timeRequired := latencyMap[customerKey].Value() * float64(RouterInMemoryJobCounts["router"][customerKey][destType]) * float64(routerTimeOut/time.Second)
		if timeRequired < runningTimeCounter {
			customerPickUpCount[customerKey] += int(timeRequired / latencyMap[customerKey].Value())
			runningTimeCounter = runningTimeCounter - timeRequired
			runningJobCount = runningJobCount - int(timeRequired/latencyMap[customerKey].Value())
			if runningJobCount <= 0 {
				pkgLogger.Infof(`[Router Pickup] Total Jobs picked up crosses the maxJobQueryBatchSize after picking pileUp`)
				return customerPickUpCount
			}
		}
	}

	return customerPickUpCount
}

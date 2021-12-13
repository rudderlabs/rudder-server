package multitenant

import (
	"bytes"
	"encoding/gob"
	"math"
	"os"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var pkgLogger logger.LoggerI
var multitenantStat MultitenantStatsT

type MultitenantStatsT struct {
	RouterInMemoryJobCounts map[string]map[string]map[string]int
	routerJobCountMutex     sync.RWMutex
	RouterInputRates        map[string]map[string]map[string]misc.MovingAverage
}

func Init() {
	multitenantStat = MultitenantStatsT{}
	pkgLogger = logger.NewLogger().Child("services").Child("multitenant")
	multitenantStat.RouterInMemoryJobCounts = make(map[string]map[string]map[string]int)
	multitenantStat.RouterInMemoryJobCounts["router"] = make(map[string]map[string]int)
	multitenantStat.RouterInMemoryJobCounts["batch_router"] = make(map[string]map[string]int)
	prePopulateRouterPileUpCounts()
	multitenantStat.RouterInputRates = make(map[string]map[string]map[string]misc.MovingAverage)
	multitenantStat.RouterInputRates["router"] = make(map[string]map[string]misc.MovingAverage)
	multitenantStat.RouterInputRates["batch_router"] = make(map[string]map[string]misc.MovingAverage)
	go writerouterPileUpStatsEncodedToFile()
}

func writerouterPileUpStatsEncodedToFile() {
	for {

		tmpDirPath, err := misc.CreateTMPDIR()
		if err != nil {
			panic(err)
		}
		path := tmpDirPath + "router_pile_up_stat_persist.txt"
		file, _ := os.Create(path)
		buf := new(bytes.Buffer)
		encoder := gob.NewEncoder(buf)
		err = encoder.Encode(multitenantStat.RouterInMemoryJobCounts)
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
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	path := tmpDirPath + "router_pile_up_stat_persist.txt"
	byteData, err := os.ReadFile(path)
	if err != nil {
		//TODO : Build Stats with CrashRecover Query If file not found
		return
	}
	if len(byteData) == 0 {
		return
	}
	bufferedData := bytes.NewBuffer(byteData)
	decoder := gob.NewDecoder(bufferedData)
	err = decoder.Decode(&multitenantStat.RouterInMemoryJobCounts)
	if err != nil {
		panic(err)
	}
}

func AddToInMemoryCount(customerID string, destinationType string, count int, tableType string) {
	multitenantStat.routerJobCountMutex.RLock()
	_, ok := multitenantStat.RouterInMemoryJobCounts[tableType][customerID]
	if !ok {
		multitenantStat.routerJobCountMutex.RUnlock()
		multitenantStat.routerJobCountMutex.Lock()
		multitenantStat.RouterInMemoryJobCounts[tableType][customerID] = make(map[string]int)
		multitenantStat.routerJobCountMutex.Unlock()
		multitenantStat.routerJobCountMutex.RLock()
	}
	multitenantStat.routerJobCountMutex.RUnlock()
	multitenantStat.RouterInMemoryJobCounts[tableType][customerID][destinationType] += count
}

func RemoveFromInMemoryCount(customerID string, destinationType string, count int, tableType string) {
	multitenantStat.routerJobCountMutex.RLock()
	_, ok := multitenantStat.RouterInMemoryJobCounts[tableType][customerID]
	if !ok {
		multitenantStat.routerJobCountMutex.RUnlock()
		multitenantStat.routerJobCountMutex.Lock()
		multitenantStat.RouterInMemoryJobCounts[tableType][customerID] = make(map[string]int)
		multitenantStat.routerJobCountMutex.Unlock()
		multitenantStat.routerJobCountMutex.RLock()
	}
	multitenantStat.routerJobCountMutex.RUnlock()
	multitenantStat.RouterInMemoryJobCounts[tableType][customerID][destinationType] += -1 * count
}

func ReportProcLoopAddStats(stats map[string]map[string]int, timeTaken time.Duration, tableType string) {
	for key := range stats {
		multitenantStat.routerJobCountMutex.RLock()
		_, ok := multitenantStat.RouterInputRates[tableType][key]
		if !ok {
			multitenantStat.routerJobCountMutex.RUnlock()
			multitenantStat.routerJobCountMutex.Lock()
			multitenantStat.RouterInputRates[tableType][key] = make(map[string]misc.MovingAverage)
			multitenantStat.routerJobCountMutex.Unlock()
			multitenantStat.routerJobCountMutex.RLock()
		}
		multitenantStat.routerJobCountMutex.RUnlock()
		for destType := range stats[key] {
			multitenantStat.routerJobCountMutex.RLock()
			_, ok := multitenantStat.RouterInputRates[tableType][key][destType]
			if !ok {
				multitenantStat.routerJobCountMutex.RUnlock()
				multitenantStat.routerJobCountMutex.Lock()
				multitenantStat.RouterInputRates[tableType][key][destType] = misc.NewMovingAverage()
				multitenantStat.routerJobCountMutex.Unlock()
				multitenantStat.routerJobCountMutex.RLock()
			}
			multitenantStat.routerJobCountMutex.RUnlock()
			multitenantStat.RouterInputRates[tableType][key][destType].Add(float64(stats[key][destType]) * float64(time.Second) / float64(timeTaken))
			AddToInMemoryCount(key, destType, stats[key][destType], tableType)
		}
	}
	for customerKey := range multitenantStat.RouterInputRates[tableType] {
		_, ok := stats[customerKey]
		if !ok {
			for destType := range stats[customerKey] {
				multitenantStat.routerJobCountMutex.Lock()
				multitenantStat.RouterInputRates[tableType][customerKey][destType].Add(0)
				multitenantStat.routerJobCountMutex.Unlock()
			}
		}

		for destType := range multitenantStat.RouterInputRates[tableType][customerKey] {
			_, ok := stats[customerKey][destType]
			if !ok {
				multitenantStat.routerJobCountMutex.Lock()
				multitenantStat.RouterInputRates[tableType][customerKey][destType].Add(0)
				multitenantStat.routerJobCountMutex.Unlock()
			}
		}
	}
}

func GetRouterPickupJobs(destType string, earliestJobMap map[string]time.Time, sortedLatencyList []string, noOfWorkers int, routerTimeOut time.Duration, latencyMap map[string]misc.MovingAverage, jobQueryBatchSize int) map[string]int {
	customerPickUpCount := make(map[string]int)
	runningTimeCounter := float64(noOfWorkers) * float64(routerTimeOut) / float64(time.Second)
	multitenantStat.routerJobCountMutex.RLock()
	defer multitenantStat.routerJobCountMutex.RUnlock()
	runningJobCount := jobQueryBatchSize

	for _, customerKey := range sortedLatencyList {
		customerCountKey, ok := multitenantStat.RouterInputRates["router"][customerKey]
		if ok {
			destTypeCount, ok := customerCountKey[destType]
			if ok {
				timeRequired := 0.0
				if latencyMap[customerKey].Value() != 0 {
					timeRequired = float64(latencyMap[customerKey].Value() * destTypeCount.Value() * float64(routerTimeOut) / float64(time.Second))
					customerPickUpCount[customerKey] = int(math.Min(timeRequired, runningTimeCounter) / latencyMap[customerKey].Value())
					if customerPickUpCount[customerKey] == 0 && multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType] > 0 {
						customerPickUpCount[customerKey] = customerPickUpCount[customerKey] + 1
					}
					if customerPickUpCount[customerKey] > multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType] {
						customerPickUpCount[customerKey] = multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType]
						timeRequired = latencyMap[customerKey].Value() * float64(customerPickUpCount[customerKey]) * float64(routerTimeOut) / float64(time.Second)
					}
				} else {
					customerPickUpCount[customerKey] = misc.MinInt(int(destTypeCount.Value()), multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType])
				}
				runningTimeCounter = runningTimeCounter - timeRequired
				runningJobCount = runningJobCount - customerPickUpCount[customerKey]
				//runningJobCount should be a number large enough to ensure fairness but small enough to not cause OOM Issues
				if runningJobCount <= 0 || runningTimeCounter <= 0 {
					pkgLogger.Infof(`[Router Pickup] Total Jobs picked up crosses the maxJobQueryBatchSize for %v with count %v`, destType, runningJobCount)
					return customerPickUpCount
				}
			}
		}
	}

	for _, customerKey := range sortedLatencyList {
		customerCountKey, ok := multitenantStat.RouterInMemoryJobCounts["router"][customerKey]
		if !ok {
			continue
		}
		if customerCountKey[destType] == 0 {
			continue
		}
		timeRequired := latencyMap[customerKey].Value() * float64(customerCountKey[destType]) * float64(routerTimeOut) / float64(time.Second)
		if timeRequired < runningTimeCounter {
			if latencyMap[customerKey].Value() != 0 {
				pickUpCount := misc.MinInt(int(timeRequired/latencyMap[customerKey].Value()), runningJobCount)
				customerPickUpCount[customerKey] += pickUpCount
				runningTimeCounter = runningTimeCounter - timeRequired
				runningJobCount = runningJobCount - pickUpCount
				if runningJobCount <= 0 || runningTimeCounter <= 0 {
					pkgLogger.Infof(`[Router Pickup] Total Jobs picked up crosses the maxJobQueryBatchSize after picking pileUp for %v with count %v`, destType, runningJobCount)
					return customerPickUpCount
				}
			}
		} else {
			pickUpCount := int(runningTimeCounter / latencyMap[customerKey].Value())
			customerPickUpCount[customerKey] += pickUpCount
			runningJobCount = runningJobCount - pickUpCount
			pkgLogger.Infof(`[Router Pickup] Total Jobs picked exhausted the time limit after picking pileUp for %v with count %v`, destType, runningJobCount)
			return customerPickUpCount
		}
	}

	return customerPickUpCount
}

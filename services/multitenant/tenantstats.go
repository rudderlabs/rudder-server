package multitenant

import (
	"bytes"
	"encoding/gob"
	"math"
	"os"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/services/stats"
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
	// prePopulateRouterPileUpCounts()
	multitenantStat.RouterInputRates = make(map[string]map[string]map[string]misc.MovingAverage)
	multitenantStat.RouterInputRates["router"] = make(map[string]map[string]misc.MovingAverage)
	multitenantStat.RouterInputRates["batch_router"] = make(map[string]map[string]misc.MovingAverage)
	hardcodeInMemoryStats()
	// go writerouterPileUpStatsEncodedToFile()
	go SendRouterInMovingAverageStat()
	go SendPileUpStats()
}

func hardcodeInMemoryStats() {

	multitenantStat.RouterInputRates["router"]["22XEji7vy1kqt9cOlRs3sqDL5yC"] = make(map[string]misc.MovingAverage)
	multitenantStat.RouterInputRates["router"]["22XEpPF6GSeLXQnYnZTCetLA6pT"] = make(map[string]misc.MovingAverage)
	multitenantStat.RouterInputRates["router"]["22XEpPF6GSeLXQnYnZTCetLA6pT"] = make(map[string]misc.MovingAverage)
	multitenantStat.RouterInputRates["router"]["22XEqwbFowYt87cGPOdmARbK99b"] = make(map[string]misc.MovingAverage)
	multitenantStat.RouterInputRates["router"]["22XEsORvYnOy1zeAbfQmXVb1Tie"] = make(map[string]misc.MovingAverage)
	multitenantStat.RouterInputRates["router"]["22XEtxIwdZgVO4tAxF8nLtNHEAM"] = make(map[string]misc.MovingAverage)
	multitenantStat.RouterInputRates["router"]["22XEvPN3TUqbpAUequb6W4HJDJ0"] = make(map[string]misc.MovingAverage)
	multitenantStat.RouterInputRates["router"]["22XEwlX8YitV9aWbK4Fj8QabxeY"] = make(map[string]misc.MovingAverage)
	multitenantStat.RouterInputRates["router"]["22XFT1wZ6MEe2YDXLeBspRKs1Nn"] = make(map[string]misc.MovingAverage)
	multitenantStat.RouterInputRates["router"]["22XFUKHDiLXirhVsuLOWv2zwAj3"] = make(map[string]misc.MovingAverage)
	multitenantStat.RouterInputRates["router"]["22XFVrf23GDmtVCsAQqJwewTgW5"] = make(map[string]misc.MovingAverage)

	multitenantStat.RouterInputRates["router"]["22XEji7vy1kqt9cOlRs3sqDL5yC"]["WEBHOOK"] = misc.NewMovingAverage()
	multitenantStat.RouterInputRates["router"]["22XEpPF6GSeLXQnYnZTCetLA6pT"]["WEBHOOK"] = misc.NewMovingAverage()
	multitenantStat.RouterInputRates["router"]["22XEpPF6GSeLXQnYnZTCetLA6pT"]["WEBHOOK"] = misc.NewMovingAverage()
	multitenantStat.RouterInputRates["router"]["22XEqwbFowYt87cGPOdmARbK99b"]["WEBHOOK"] = misc.NewMovingAverage()
	multitenantStat.RouterInputRates["router"]["22XEsORvYnOy1zeAbfQmXVb1Tie"]["WEBHOOK"] = misc.NewMovingAverage()
	multitenantStat.RouterInputRates["router"]["22XEtxIwdZgVO4tAxF8nLtNHEAM"]["WEBHOOK"] = misc.NewMovingAverage()
	multitenantStat.RouterInputRates["router"]["22XEvPN3TUqbpAUequb6W4HJDJ0"]["WEBHOOK"] = misc.NewMovingAverage()
	multitenantStat.RouterInputRates["router"]["22XEwlX8YitV9aWbK4Fj8QabxeY"]["WEBHOOK"] = misc.NewMovingAverage()
	multitenantStat.RouterInputRates["router"]["22XFT1wZ6MEe2YDXLeBspRKs1Nn"]["WEBHOOK"] = misc.NewMovingAverage()
	multitenantStat.RouterInputRates["router"]["22XFUKHDiLXirhVsuLOWv2zwAj3"]["WEBHOOK"] = misc.NewMovingAverage()
	multitenantStat.RouterInputRates["router"]["22XFVrf23GDmtVCsAQqJwewTgW5"]["WEBHOOK"] = misc.NewMovingAverage()

	multitenantStat.RouterInMemoryJobCounts["router"]["22XEji7vy1kqt9cOlRs3sqDL5yC"] = make(map[string]int)
	multitenantStat.RouterInMemoryJobCounts["router"]["22XEpPF6GSeLXQnYnZTCetLA6pT"] = make(map[string]int)
	multitenantStat.RouterInMemoryJobCounts["router"]["22XFT1wZ6MEe2YDXLeBspRKs1Nn"] = make(map[string]int)
	multitenantStat.RouterInMemoryJobCounts["router"]["22XEqwbFowYt87cGPOdmARbK99b"] = make(map[string]int)
	multitenantStat.RouterInMemoryJobCounts["router"]["22XEsORvYnOy1zeAbfQmXVb1Tie"] = make(map[string]int)
	multitenantStat.RouterInMemoryJobCounts["router"]["22XEtxIwdZgVO4tAxF8nLtNHEAM"] = make(map[string]int)
	multitenantStat.RouterInMemoryJobCounts["router"]["22XEvPN3TUqbpAUequb6W4HJDJ0"] = make(map[string]int)
	multitenantStat.RouterInMemoryJobCounts["router"]["22XEwlX8YitV9aWbK4Fj8QabxeY"] = make(map[string]int)
	multitenantStat.RouterInMemoryJobCounts["router"]["22XFUKHDiLXirhVsuLOWv2zwAj3"] = make(map[string]int)
	multitenantStat.RouterInMemoryJobCounts["router"]["22XFVrf23GDmtVCsAQqJwewTgW5"] = make(map[string]int)

	multitenantStat.RouterInMemoryJobCounts["router"]["22XEji7vy1kqt9cOlRs3sqDL5yC"]["WEBHOOK"] = 0
	multitenantStat.RouterInMemoryJobCounts["router"]["22XEpPF6GSeLXQnYnZTCetLA6pT"]["WEBHOOK"] = 0
	multitenantStat.RouterInMemoryJobCounts["router"]["22XFT1wZ6MEe2YDXLeBspRKs1Nn"]["WEBHOOK"] = 0
	multitenantStat.RouterInMemoryJobCounts["router"]["22XEqwbFowYt87cGPOdmARbK99b"]["WEBHOOK"] = 0
	multitenantStat.RouterInMemoryJobCounts["router"]["22XEsORvYnOy1zeAbfQmXVb1Tie"]["WEBHOOK"] = 0
	multitenantStat.RouterInMemoryJobCounts["router"]["22XEtxIwdZgVO4tAxF8nLtNHEAM"]["WEBHOOK"] = 0
	multitenantStat.RouterInMemoryJobCounts["router"]["22XEvPN3TUqbpAUequb6W4HJDJ0"]["WEBHOOK"] = 0
	multitenantStat.RouterInMemoryJobCounts["router"]["22XEwlX8YitV9aWbK4Fj8QabxeY"]["WEBHOOK"] = 0
	multitenantStat.RouterInMemoryJobCounts["router"]["22XFUKHDiLXirhVsuLOWv2zwAj3"]["WEBHOOK"] = 0
	multitenantStat.RouterInMemoryJobCounts["router"]["22XFVrf23GDmtVCsAQqJwewTgW5"]["WEBHOOK"] = 0

}

func SendPileUpStats() {
	for {
		time.Sleep(10 * time.Second)
		multitenantStat.routerJobCountMutex.RLock()
		for customer, value := range multitenantStat.RouterInMemoryJobCounts["router"] {
			for destType, count := range value {
				countStat := stats.NewTaggedStat("pile_up_count", stats.GaugeType, stats.Tags{
					"customer": customer,
					"destType": destType,
				})
				countStat.Gauge(count)
				pkgLogger.Infof("pile_up_count is %v for customer %v", count, customer)
			}
		}
		multitenantStat.routerJobCountMutex.RUnlock()
	}
}

func SendRouterInMovingAverageStat() {
	for {
		time.Sleep(10 * time.Second)
		multitenantStat.routerJobCountMutex.RLock()
		for customer, value := range multitenantStat.RouterInputRates["router"] {
			for destType, count := range value {
				movingAverageStat := stats.NewTaggedStat("router_input_rate", stats.GaugeType, stats.Tags{
					"customer": customer,
					"destType": destType,
				})
				movingAverageStat.Gauge(count.Value())
				pkgLogger.Infof("router_input_rate is %.8f for customer %v", count.Value(), customer)
			}
		}
		multitenantStat.routerJobCountMutex.RUnlock()
	}
}

func writerouterPileUpStatsEncodedToFile() {
	for {
		//TODO : Write this to DB instead of file in a transaction Or Make a DB Query Instead
		tmpDirPath, err := misc.CreateTMPDIR()
		if err != nil {
			panic(err)
		}
		path := tmpDirPath + "/router_pile_up_stat_persist.txt"
		file, _ := os.Create(path)
		buf := new(bytes.Buffer)
		encoder := gob.NewEncoder(buf)
		err = encoder.Encode(multitenantStat.RouterInMemoryJobCounts)
		if err != nil {
			panic(err)
		}
		if len(buf.Bytes()) != 0 {
			time.Sleep(10 * time.Second)
		}
		_, err = file.Write(buf.Bytes())
		if err != nil {
			panic(err)
		}
		time.Sleep(10 * time.Second) // TODO : Make these sleep timings configurable
	}
}

// func prePopulateRouterPileUpCounts() {
// 	tmpDirPath, err := misc.CreateTMPDIR()
// 	if err != nil {
// 		panic(err)
// 	}
// 	path := tmpDirPath + "router_pile_up_stat_persist.txt"
// 	byteData, err := os.ReadFile(path)
// 	if err != nil {
// 		//TODO : Build Stats with CrashRecover Query If file not found
// 		return
// 	}
// 	if len(byteData) == 0 {
// 		return
// 	}
// 	bufferedData := bytes.NewBuffer(byteData)
// 	decoder := gob.NewDecoder(bufferedData)
// 	err = decoder.Decode(&multitenantStat.RouterInMemoryJobCounts)
// 	if err != nil {
// 		panic(err)
// 	}
// }

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
			multitenantStat.RouterInputRates[tableType][key][destType].Add((float64(stats[key][destType]) * float64(time.Second)) / float64(timeTaken))
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
				//runningJobCount should be a number large enough to ensure fairness but small enough to not cause OOM Issues
				if runningJobCount <= 0 || runningTimeCounter <= 0 {
					if multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType] > 0 {
						customerPickUpCount[customerKey] += 1
					}
					continue
				}
				if latencyMap[customerKey].Value() != 0 {
					timeRequired = float64(latencyMap[customerKey].Value() * destTypeCount.Value() * float64(routerTimeOut) / float64(time.Second))
					customerPickUpCount[customerKey] = int(math.Min(timeRequired, runningTimeCounter) / latencyMap[customerKey].Value())
					// if customerPickUpCount[customerKey] == 0 && multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType] > 0 {
					// 	customerPickUpCount[customerKey] = customerPickUpCount[customerKey] + 1
					// }
					if customerPickUpCount[customerKey] > multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType] {
						customerPickUpCount[customerKey] = misc.MaxInt(multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType], 0)
						timeRequired = latencyMap[customerKey].Value() * float64(customerPickUpCount[customerKey])
					}
				} else {
					customerPickUpCount[customerKey] = misc.MinInt(int(destTypeCount.Value()*float64(routerTimeOut)/float64(time.Second)), multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType])
				}
				runningTimeCounter = runningTimeCounter - timeRequired
				runningJobCount = runningJobCount - customerPickUpCount[customerKey]
				pkgLogger.Infof("Time Calculated : %v , Remaining Time : %v , Customer : %v ,runningJobCount : %v , InRateLoop ", timeRequired, runningTimeCounter, customerKey, runningJobCount)
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
		if runningJobCount <= 0 || runningTimeCounter <= 0 {
			if multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType] > 0 && customerPickUpCount[customerKey] == 0 {
				customerPickUpCount[customerKey] += 1
			}
			continue
		}

		timeRequired := latencyMap[customerKey].Value() * float64(customerCountKey[destType])
		//TODO : Include earliestJobMap into the algorithm if required or get away with earliestJobMap
		if timeRequired < runningTimeCounter {
			if latencyMap[customerKey].Value() != 0 {
				pickUpCount := misc.MinInt(customerCountKey[destType]-customerPickUpCount[destType], runningJobCount)
				customerPickUpCount[customerKey] += pickUpCount
				runningTimeCounter = runningTimeCounter - timeRequired
				runningJobCount = runningJobCount - pickUpCount
			}
			// Migrated jobs fix in else condition
		} else {
			pickUpCount := int(runningTimeCounter / latencyMap[customerKey].Value())
			pileupPickUp := misc.MinInt(misc.MinInt(pickUpCount, runningJobCount), customerCountKey[destType]-customerPickUpCount[destType])
			customerPickUpCount[customerKey] += pileupPickUp
			runningJobCount = runningJobCount - pileupPickUp
			runningTimeCounter = runningTimeCounter - float64(pileupPickUp)*latencyMap[customerKey].Value()
		}
		pkgLogger.Infof("Time Calculated : %v , Remaining Time : %v , Customer : %v ,runningJobCount : %v , PileUpLoop ", timeRequired, runningTimeCounter, customerKey, runningJobCount)
	}

	///TODO : Remove these logs when merging to master
	pkgLogger.Infof("latencyMap: %v,%v,%v,%v,%v,%v,%v,%v,%v,%v", latencyMap["22XEji7vy1kqt9cOlRs3sqDL5yC"].Value(), latencyMap["22XEpPF6GSeLXQnYnZTCetLA6pT"].Value(), latencyMap["22XEqwbFowYt87cGPOdmARbK99b"].Value(), latencyMap["22XEsORvYnOy1zeAbfQmXVb1Tie"].Value(), latencyMap["22XEtxIwdZgVO4tAxF8nLtNHEAM"].Value(), latencyMap["22XEvPN3TUqbpAUequb6W4HJDJ0"].Value(), latencyMap["22XEwlX8YitV9aWbK4Fj8QabxeY"].Value(), latencyMap["22XFT1wZ6MEe2YDXLeBspRKs1Nn"].Value(), latencyMap["22XFUKHDiLXirhVsuLOWv2zwAj3"].Value(), latencyMap["22XFVrf23GDmtVCsAQqJwewTgW5"].Value())
	pkgLogger.Infof("routerInRates: %v,%v,%v,%v,%v,%v,%v,%v,%v,%v", multitenantStat.RouterInputRates["router"]["22XEji7vy1kqt9cOlRs3sqDL5yC"][destType].Value(), multitenantStat.RouterInputRates["router"]["22XEpPF6GSeLXQnYnZTCetLA6pT"][destType].Value(), multitenantStat.RouterInputRates["router"]["22XEqwbFowYt87cGPOdmARbK99b"][destType].Value(), multitenantStat.RouterInputRates["router"]["22XEsORvYnOy1zeAbfQmXVb1Tie"][destType].Value(), multitenantStat.RouterInputRates["router"]["22XEtxIwdZgVO4tAxF8nLtNHEAM"][destType].Value(), multitenantStat.RouterInputRates["router"]["22XEvPN3TUqbpAUequb6W4HJDJ0"][destType].Value(), multitenantStat.RouterInputRates["router"]["22XEwlX8YitV9aWbK4Fj8QabxeY"][destType].Value(), multitenantStat.RouterInputRates["router"]["22XFT1wZ6MEe2YDXLeBspRKs1Nn"][destType].Value(), multitenantStat.RouterInputRates["router"]["22XFUKHDiLXirhVsuLOWv2zwAj3"][destType].Value(), multitenantStat.RouterInputRates["router"]["22XFVrf23GDmtVCsAQqJwewTgW5"][destType].Value())
	pkgLogger.Infof("pileUpStats : %v,%v,%v,%v,%v,%v,%v,%v,%v,%v", multitenantStat.RouterInMemoryJobCounts["router"]["22XEji7vy1kqt9cOlRs3sqDL5yC"][destType], multitenantStat.RouterInMemoryJobCounts["router"]["22XEpPF6GSeLXQnYnZTCetLA6pT"][destType], multitenantStat.RouterInMemoryJobCounts["router"]["22XEqwbFowYt87cGPOdmARbK99b"][destType], multitenantStat.RouterInMemoryJobCounts["router"]["22XEsORvYnOy1zeAbfQmXVb1Tie"][destType], multitenantStat.RouterInMemoryJobCounts["router"]["22XEtxIwdZgVO4tAxF8nLtNHEAM"][destType], multitenantStat.RouterInMemoryJobCounts["router"]["22XEvPN3TUqbpAUequb6W4HJDJ0"][destType], multitenantStat.RouterInMemoryJobCounts["router"]["22XEwlX8YitV9aWbK4Fj8QabxeY"][destType], multitenantStat.RouterInMemoryJobCounts["router"]["22XFT1wZ6MEe2YDXLeBspRKs1Nn"][destType], multitenantStat.RouterInMemoryJobCounts["router"]["22XFUKHDiLXirhVsuLOWv2zwAj3"][destType], multitenantStat.RouterInMemoryJobCounts["router"]["22XFVrf23GDmtVCsAQqJwewTgW5"][destType])
	pkgLogger.Infof("customerPickUpCount: %v,%v,%v,%v,%v,%v,%v,%v,%v,%v", customerPickUpCount["22XEji7vy1kqt9cOlRs3sqDL5yC"], customerPickUpCount["22XEpPF6GSeLXQnYnZTCetLA6pT"], customerPickUpCount["22XEqwbFowYt87cGPOdmARbK99b"], customerPickUpCount["22XEsORvYnOy1zeAbfQmXVb1Tie"], customerPickUpCount["22XEtxIwdZgVO4tAxF8nLtNHEAM"], customerPickUpCount["22XEvPN3TUqbpAUequb6W4HJDJ0"], customerPickUpCount["22XEwlX8YitV9aWbK4Fj8QabxeY"], customerPickUpCount["22XFT1wZ6MEe2YDXLeBspRKs1Nn"], customerPickUpCount["22XFUKHDiLXirhVsuLOWv2zwAj3"], customerPickUpCount["22XFVrf23GDmtVCsAQqJwewTgW5"])

	return customerPickUpCount
}

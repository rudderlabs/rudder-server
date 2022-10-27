package main

import (
	"fmt"
	"time"
)

type sourceFilter struct {
	all      bool
	specific map[string]struct{}
}
type sourceRegulation struct {
	Canceled  bool     `json:"canceled"`
	UserID    string   `json:"userId"`
	SourceIDs []string `json:"sourceIds"`
}

func insert(regulations []sourceRegulation, suppressionMap map[string]sourceFilter) {
	for _, sourceRegulation := range regulations {
		userId := sourceRegulation.UserID
		if len(sourceRegulation.SourceIDs) == 0 {
			if _, ok := suppressionMap[userId]; !ok {
				if !sourceRegulation.Canceled {
					m := sourceFilter{
						all:      true,
						specific: map[string]struct{}{},
					}
					suppressionMap[userId] = m
					continue
				}
			}
			m := suppressionMap[userId]
			if sourceRegulation.Canceled {
				m.all = false
			} else {
				m.all = true
			}
			suppressionMap[userId] = m
		} else {
			if _, ok := suppressionMap[userId]; !ok {
				if !sourceRegulation.Canceled {
					m := sourceFilter{
						specific: map[string]struct{}{},
					}
					for _, srcId := range sourceRegulation.SourceIDs {
						m.specific[srcId] = struct{}{}
					}
					suppressionMap[userId] = m
					continue
				}
			}
			m := suppressionMap[userId]
			if sourceRegulation.Canceled {
				for _, srcId := range sourceRegulation.SourceIDs {
					delete(m.specific, srcId) // will be no-op if key is not there in map
				}
			} else {
				for _, srcId := range sourceRegulation.SourceIDs {
					m.specific[srcId] = struct{}{}
				}
			}
			suppressionMap[userId] = m
		}
	}
}

func createMockRegulation(regCount int) []sourceRegulation {
	regulations := make([]sourceRegulation, regCount)
	for i := 0; i < regCount; i++ {
		regulations[i] = sourceRegulation{
			Canceled:  false,
			UserID:    fmt.Sprint(i),
			SourceIDs: []string{fmt.Sprint(i)},
		}
	}
	return regulations
}
func main() {
	// map1 := map[string]sourceFilter{}
	// time1 := time.Now()
	// for i := 10000000; i < 50000000; i++ {
	// 	if _, ok := map1[fmt.Sprint(i)]; !ok {
	// 		map1[fmt.Sprint(i)] = sourceFilter{}
	// 	}
	// }
	// fmt.Println("init without size, time to insert 1M events: ", time.Since(time1))

	// map2 := make(map[string]sourceFilter, 40000000)

	// time2 := time.Now()
	// for i := 10000000; i < 50000000; i++ {
	// 	if _, ok := map2[fmt.Sprint(i)]; !ok {
	// 		// map1[fmt.Sprint(i)] = sourceFilter{specific: make(map[string]struct{}, 100)}
	// 		map1[fmt.Sprint(i)] = sourceFilter{}
	// 	}
	// }
	// fmt.Println("init with size, time to insert 1M events: ", time.Since(time2))
	// var regulations []sourceRegulation

	userCount := 10000000
	regulations := createMockRegulation(userCount)
	// uninitSuppressionMap := map[string]sourceFilter{}
	// uninitTime := time.Now()
	// insert(regulations, uninitSuppressionMap)
	// fmt.Printf("uninit map, time to insert %d events: %s\n", userCount, time.Since(uninitTime))
	initSuppressionMap := make(map[string]sourceFilter, userCount)
	initTime := time.Now()
	insert(regulations, initSuppressionMap)
	fmt.Printf("init map, time to insert %d events: %s\n", userCount, time.Since(initTime))

}

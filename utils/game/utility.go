package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
)

func main() {
	gameFile, err := os.Open("game.json")
	if err != nil {
		fmt.Println("error opening file")
		return
	}
	rudderFile, errNew := os.Open("rudder.json")
	if errNew != nil {
		fmt.Println("error opening file")
		return
	}

	reader := bufio.NewReader(gameFile)

	b, err := reader.ReadBytes('\n')

	proccessed := 0
	gameInsertIDToFound := make(map[string]bool)
	insertIDToEventPropertiesMap := make(map[string]map[string]interface{})
	var gameInserIDOrder []string
	var unmarhsalledData map[string]interface{}
	var propsNotMatch []string
	var inserIDNotPresent []string

	for err != io.EOF {
		json.Unmarshal(b, &unmarhsalledData)
		eventProperties := unmarhsalledData["event_properties"].(map[string]interface{})
		insertID := eventProperties["insert_id"].(string)
		insertIDToEventPropertiesMap[insertID] = eventProperties
		gameInsertIDToFound[insertID] = false
		gameInserIDOrder = append(gameInserIDOrder, insertID)
		proccessed++
		b, err = reader.ReadBytes('\n')
	}

	fmt.Println("game number of insertids: ", proccessed)

	reader.Reset(rudderFile)
	proccessed = 0
	order := 0
	props := 0

	b, err = reader.ReadBytes('\n')
	for err != io.EOF {
		json.Unmarshal(b, &unmarhsalledData)
		eventProperties := unmarhsalledData["event_properties"].(map[string]interface{})
		insertID := unmarhsalledData["$insert_id"].(string)

		// see if insert id present in game
		_, ok := gameInsertIDToFound[insertID]
		if !ok {
			fmt.Println("insert id in rudder not found in game: ", insertID)
			inserIDNotPresent = append(inserIDNotPresent, insertID)
		} else {
			// see if insert id order match
			if gameInserIDOrder[proccessed] == insertID {
				// fmt.Println("insert id order match!!!")
				order++
			} else {
				fmt.Println("insert id out of order: ", gameInserIDOrder[proccessed], " ", insertID)
			}

			gameInsertIDToFound[insertID] = true

			// see if event props match
			delete(insertIDToEventPropertiesMap[insertID], "insert_id")
			if reflect.DeepEqual(eventProperties, insertIDToEventPropertiesMap[insertID]) {
				//fmt.Println("event props match: ", true)
				props++
			} else {
				fmt.Println("event props match: ", false)
				propsNotMatch = append(propsNotMatch, insertID)

			}

		}
		proccessed++

		b, err = reader.ReadBytes('\n')
	}

	fmt.Println("order matched: ", order)
	fmt.Println("props matched: ", props)
	fmt.Println(" check these insertIds, props don't match: ", len(propsNotMatch), propsNotMatch)
	fmt.Println(" check these insertIds, ids not present in game: ", len(inserIDNotPresent), inserIDNotPresent)

	fmt.Println(" check there insertIds, ids not present in rudder ")

	notFound := 0
	for k, v := range gameInsertIDToFound {
		if !v {
			notFound++
			fmt.Println(k)
		}
	}
	fmt.Println(notFound)
}

package main

import (
	"flag"
	"log"
	"os"
	"reflect"

	"github.com/BurntSushi/toml"
)

func init() {
	flag.Parse()
}

func merge(output map[string]interface{}, tmp map[string]interface{}) {
	for k, v := range tmp {
		if reflect.TypeOf(v).Kind() == reflect.Map {
			if output[k] == nil {
				output[k] = make(map[string]interface{})
			}
			merge(output[k].(map[string]interface{}), v.(map[string]interface{}))
		} else {
			output[k] = v
		}
	}
}

func main() {
	var output = make(map[string]interface{})

	for _, f := range flag.Args() {
		var tmp map[string]interface{}
		_, err := toml.DecodeFile(f, &tmp)
		if err != nil {
			log.Fatalf("Error in '%s': %s", f, err)
		}
		merge(output, tmp)
	}

	tomlrt := toml.NewEncoder(os.Stdout)
	tomlrt.Encode(output)
}

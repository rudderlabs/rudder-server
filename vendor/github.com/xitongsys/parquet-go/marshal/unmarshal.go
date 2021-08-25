package marshal

import (
	"errors"
	"reflect"
	"strings"

	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
)

//Record Map KeyValue pair
type KeyValue struct {
	Key   reflect.Value
	Value reflect.Value
}

type MapRecord struct {
	KeyValues []KeyValue
	Index     int
}

type SliceRecord struct {
	Values []reflect.Value
	Index  int
}

//Convert the table map to objects slice. desInterface is a slice of pointers of objects
func Unmarshal(tableMap *map[string]*layout.Table, bgn int, end int, dstInterface interface{}, schemaHandler *schema.SchemaHandler, prefixPath string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("unknown error")
			}
		}
	}()

	tableNeeds := make(map[string]*layout.Table)
	tableBgn, tableEnd := make(map[string]int), make(map[string]int)
	for name, table := range *tableMap {
		if !strings.HasPrefix(name, prefixPath) {
			continue
		}

		tableNeeds[name] = table

		ln := len(table.Values)
		num := -1
		tableBgn[name], tableEnd[name] = -1, -1
		for i := 0; i < ln; i++ {
			if table.RepetitionLevels[i] == 0 {
				num++
				if num == bgn {
					tableBgn[name] = i
				}
				if num == end {
					tableEnd[name] = i
					break
				}
			}
		}

		if tableEnd[name] < 0 {
			tableEnd[name] = ln
		}
		if tableBgn[name] < 0 {
			return
		}
	}

	mapRecords := make(map[reflect.Value]*MapRecord)
	mapRecordsStack := make([]reflect.Value, 0)
	sliceRecords := make(map[reflect.Value]*SliceRecord)
	sliceRecordsStack := make([]reflect.Value, 0)
	root := reflect.ValueOf(dstInterface).Elem()
	prefixIndex := common.PathStrIndex(prefixPath) - 1

	for name, table := range tableNeeds {
		path := table.Path
		bgn := tableBgn[name]
		end := tableEnd[name]
		schemaIndexs := make([]int, len(path))
		for i := 0; i < len(path); i++ {
			curPathStr := common.PathToStr(path[:i+1])
			schemaIndexs[i] = int(schemaHandler.MapIndex[curPathStr])
		}

		repetitionLevels, definitionLevels := make([]int32, len(path)), make([]int32, len(path))
		for i := 0; i < len(path); i++ {
			repetitionLevels[i], _ = schemaHandler.MaxRepetitionLevel(path[:i+1])
			definitionLevels[i], _ = schemaHandler.MaxDefinitionLevel(path[:i+1])
		}

		for _, rc := range sliceRecords {
			rc.Index = -1
		}
		for _, rc := range mapRecords {
			rc.Index = -1
		}

		for i := bgn; i < end; i++ {
			rl, dl, val := table.RepetitionLevels[i], table.DefinitionLevels[i], table.Values[i]
			po, index := root, prefixIndex
			for index < len(path) {
				schemaIndex := schemaIndexs[index]
				_, cT := schemaHandler.SchemaElements[schemaIndex].Type, schemaHandler.SchemaElements[schemaIndex].ConvertedType

				if po.Type().Kind() == reflect.Slice && (cT == nil || *cT != parquet.ConvertedType_LIST) {
					if po.IsNil() {
						po.Set(reflect.MakeSlice(po.Type(), 0, 0))
					}

					if _, ok := sliceRecords[po]; !ok {
						sliceRecords[po] = &SliceRecord{
							Values: []reflect.Value{},
							Index:  -1,
						}
						sliceRecordsStack = append(sliceRecordsStack, po)
					}

					if rl == repetitionLevels[index] || sliceRecords[po].Index < 0 {
						sliceRecords[po].Index++
					}

					if sliceRecords[po].Index >= len(sliceRecords[po].Values) {
						sliceRecords[po].Values = append(sliceRecords[po].Values, reflect.New(po.Type().Elem()).Elem())
					}

					po = sliceRecords[po].Values[sliceRecords[po].Index]

				} else if po.Type().Kind() == reflect.Slice && cT != nil && *cT == parquet.ConvertedType_LIST {
					if po.IsNil() {
						po.Set(reflect.MakeSlice(po.Type(), 0, 0))
					}

					if _, ok := sliceRecords[po]; !ok {
						sliceRecords[po] = &SliceRecord{
							Values: []reflect.Value{},
							Index:  -1,
						}
						sliceRecordsStack = append(sliceRecordsStack, po)
					}

					index++
					if definitionLevels[index] > dl {
						break
					}

					if rl == repetitionLevels[index] || sliceRecords[po].Index < 0 {
						sliceRecords[po].Index++
					}

					if sliceRecords[po].Index >= len(sliceRecords[po].Values) {
						sliceRecords[po].Values = append(sliceRecords[po].Values, reflect.New(po.Type().Elem()).Elem())
					}

					po = sliceRecords[po].Values[sliceRecords[po].Index]
					index++
					if definitionLevels[index] > dl {
						break
					}

				} else if po.Type().Kind() == reflect.Map {
					if po.IsNil() {
						po.Set(reflect.MakeMap(po.Type()))
					}

					if _, ok := mapRecords[po]; !ok {
						mapRecords[po] = &MapRecord{
							KeyValues: []KeyValue{},
							Index:     -1,
						}
						mapRecordsStack = append(mapRecordsStack, po)
					}

					index++
					if definitionLevels[index] > dl {
						break
					}

					if rl == repetitionLevels[index] || mapRecords[po].Index < 0 {
						mapRecords[po].Index++
					}

					if mapRecords[po].Index >= len(mapRecords[po].KeyValues) {
						mapRecords[po].KeyValues = append(mapRecords[po].KeyValues,
							KeyValue{
								Key:   reflect.New(po.Type().Key()).Elem(),
								Value: reflect.New(po.Type().Elem()).Elem(),
							})
					}

					if strings.ToLower(path[index+1]) == "key" {
						po = mapRecords[po].KeyValues[mapRecords[po].Index].Key

					} else {
						po = mapRecords[po].KeyValues[mapRecords[po].Index].Value
					}

					index++
					if definitionLevels[index] > dl {
						break
					}

				} else if po.Type().Kind() == reflect.Ptr {
					if po.IsNil() {
						po.Set(reflect.New(po.Type().Elem()))
					}

					po = po.Elem()

				} else if po.Type().Kind() == reflect.Struct {
					index++
					if definitionLevels[index] > dl {
						break
					}
					po = po.FieldByName(path[index])

				} else {
					po.Set(reflect.ValueOf(val).Convert(po.Type()))
					break
				}
			}
		}
	}

	for i := len(sliceRecordsStack) - 1; i >= 0; i-- {
		po := sliceRecordsStack[i]
		vs := sliceRecords[po]
		potmp := reflect.Append(po, vs.Values...)
		po.Set(potmp)
	}

	for i := len(mapRecordsStack) - 1; i >= 0; i-- {
		po := mapRecordsStack[i]
		for _, kv := range mapRecords[po].KeyValues {
			po.SetMapIndex(kv.Key, kv.Value)
		}
	}

	return nil
}

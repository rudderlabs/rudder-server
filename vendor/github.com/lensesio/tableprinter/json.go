package tableprinter

import (
	"encoding/json"
	"reflect"
)

type jsonParser struct{}

var byteTyp = reflect.TypeOf([]byte{0x00}[0])

func (p *jsonParser) Parse(v reflect.Value, filters []RowFilter) (headers []string, rows [][]string, nums []int) {
	var b []byte

	if kind := v.Kind(); kind == reflect.Slice {
		if v.Len() > 0 && v.Index(0).Type() == byteTyp {
			b = v.Bytes()
		} else {
			return
		}
	} else if kind == reflect.String {
		b = []byte(v.String())
	} else {
		return
	}

	var in interface{} // or map[string]interface{}
	if err := json.Unmarshal(b, &in); err != nil {
		return
	}

	if in == nil {
		return
	}

	inValue := indirectValue(reflect.ValueOf(in))
	if !inValue.IsValid() || reflect.Zero(indirectType(reflect.TypeOf(in))) == inValue {
		return
	}

	return WhichParser(inValue.Type()).Parse(inValue, filters)
}

package warehouseutils

import (
	"errors"
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/xitongsys/parquet-go/parquet"
)

type ParquetColumn struct {
	Name           string
	ParquetType    string
	ConvertedType  string
	RepetitionType string
}

func (p ParquetColumn) String() string {
	s := fmt.Sprintf("name=%[1]s, type=%[2]s, convertedtype=%[3]s", p.Name, p.ParquetType, p.ConvertedType)
	if p.RepetitionType != "" {
		s += fmt.Sprintf(", repetitiontype=%s", p.RepetitionType)
	}
	return s
}

// ParquetLoader is common for non-BQ warehouses.
// If you need any custom logic, either extend this or use destType and if/else/switch.
type ParquetLoader struct {
	destType   string
	Schema     []string
	Values     []interface{}
	FileWriter LoadFileWriterI
}

func NewParquetLoader(destType string, w LoadFileWriterI) *ParquetLoader {
	loader := &ParquetLoader{
		destType:   destType,
		FileWriter: w,
	}
	return loader
}

func (loader *ParquetLoader) IsLoadTimeColumn(columnName string) bool {
	return columnName == ToProviderCase(loader.destType, UUID_TS_COLUMN)
}

func (loader *ParquetLoader) GetLoadTimeFomat(columnName string) string {
	return misc.RFC3339Milli
}

func (loader *ParquetLoader) addColumnToSchema(name string, val interface{}) {
	// TODO: figure out if we need to add other types of columns to parquet files
	pCol := ParquetColumn{
		Name:          name,
		ParquetType:   parquet.Type_BYTE_ARRAY.String(),
		ConvertedType: parquet.ConvertedType_UTF8.String(),
	}

	if val == nil {
		pCol.RepetitionType = parquet.FieldRepetitionType_OPTIONAL.String()
	}

	loader.Schema = append(loader.Schema, pCol.String())
}

func (loader *ParquetLoader) AddColumn(columnName string, val interface{}) {
	loader.addColumnToSchema(columnName, val)
	loader.Values = append(loader.Values, val)
}

func (loader *ParquetLoader) AddRow(columnNames []string, row []string) {
	// do nothing
}

func (loader *ParquetLoader) AddEmptyColumn(columnName string) {
	loader.AddColumn(columnName, nil)
}

func (loader *ParquetLoader) WriteToString() (string, error) {
	return "", errors.New("not implemented")
}

// func (loader *ParquetLoader) WriteEventToFile() error {
// 	pqWriter, err := writer.NewCSVWriterFromWriter(loader.Schema, loader.FileWriter, 1)
// 	if err != nil {
// 		fmt.Println("writer create error", err)
// 		return err
// 	}

// 	err = pqWriter.Write(loader.Values)
// 	if err != nil {
// 		fmt.Println("write error: ", err)
// 		return err
// 	}

// 	err = pqWriter.WriteStop()
// 	if err != nil {
// 		fmt.Println("write stop error: ", err)
// 		return err
// 	}
// 	return nil
// }

func (loader *ParquetLoader) Write() error {
	return loader.FileWriter.WriteRow(loader.Values)
}

package filehandler

import (
	"context"
	"fmt"
	"reflect"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type ParquetLocalFileHandler struct {
	records []interface{}
	schema  []*parquet.SchemaElement
}

func NewParquetLocalFileHandler() *ParquetLocalFileHandler {
	return &ParquetLocalFileHandler{}
}

func (h *ParquetLocalFileHandler) Read(_ context.Context, path string) error {
	f, err := local.NewLocalFileReader(path)
	if err != nil {
		return fmt.Errorf("unable to create a new local file reader for file: %s, err: %w", path, err)
	}

	r, err := NewParquetReader(f, nil, 4)
	if err != nil {
		return fmt.Errorf("unable to create parquet reader from localfile: %s, err: %w", path, err)
	}

	defer r.ReadStop()

	entries, err := r.ReadByNumber(int(r.GetNumRows()))
	if err != nil {
		return fmt.Errorf("unable to read content from the file: %w", err)
	}

	// these entries will be filtered on in the `remove` stage.
	h.records = entries
	// schema elements will be used to create writer instance for writing entries
	h.schema = r.SchemaHandler.SchemaElements

	return nil
}

func (h *ParquetLocalFileHandler) Write(_ context.Context, path string) error {
	f, err := local.NewLocalFileWriter(path)
	if err != nil {
		return fmt.Errorf("unable to create a local file writer, %s: %w", path, err)
	}

	defer f.Close()

	w, err := writer.NewParquetWriter(f, h.schema, 4)
	if err != nil {
		return fmt.Errorf("unable to create new parquet writer: %w", err)
	}

	for _, record := range h.records {
		if err := w.Write(record); err != nil {
			return fmt.Errorf("unable to write content to the file, err: %w", err)
		}
	}

	err = w.WriteStop()
	if err != nil {
		return fmt.Errorf("unable to flush the contents to writer underneath, err: %w", err)
	}

	return nil
}

func (h *ParquetLocalFileHandler) RemoveIdentity(_ context.Context, attributes []model.User) error {
	unfiltered := make([]interface{}, 0)

	for _, record := range h.records {
		toFilterOut := false

		// As the records are struct and not pointer to struct
		// , we need to create a copy of them using the below functions.
		// In order for us to get value of the field, we need ptr to
		// struct being passed.
		elem := reflect.New(reflect.TypeOf(record))
		elem.Elem().Set(reflect.ValueOf(record))

		for _, attribute := range attributes {
			// identity matched in the record, so this
			// needs to be filtered out
			if h.identityMatched(elem.Elem(), &attribute) {
				toFilterOut = true
				break
			}
		}

		if toFilterOut {
			continue
		}
		unfiltered = append(unfiltered, record)
	}

	h.records = unfiltered
	return nil
}

func (*ParquetLocalFileHandler) identityMatched(recordValue reflect.Value, attribute *model.User) bool {
	userIdField := recordValue.FieldByName("User_id")
	if userIdField != (reflect.Value{}) {
		switch userIdField.Type().Kind() {

		// Only *string and string types are expected for the userId field
		// In case anything else is found, return with an error to be used for warnings.
		case reflect.Ptr:

			if userIdField.Elem().Type().Kind() == reflect.String {
				return userIdField.Elem().String() == attribute.ID
			}
			// pkgLogger.Debugf("unexpected data type for userId field: %v", reflect.ValueOf(userIdField).Kind())
			return false

		case reflect.String:
			return userIdField.String() == attribute.ID

		default:
			fmt.Printf("unexpected data type for userId field: %v\n", reflect.ValueOf(userIdField).Kind())
			return false

		}
	}

	return false
}

func NewParquetReader(pFile source.ParquetFile, obj interface{}, np int64) (*reader.ParquetReader, error) {
	var err error

	res := new(reader.ParquetReader)
	res.NP = np
	res.PFile = pFile
	if err = res.ReadFooter(); err != nil {
		return nil, err
	}
	res.ColumnBuffers = make(map[string]*reader.ColumnBufferType)

	if obj != nil {
		if sa, ok := obj.(string); ok {
			err = res.SetSchemaHandlerFromJSON(sa)
			return res, err

		} else if sa, ok := obj.([]*parquet.SchemaElement); ok {
			res.SchemaHandler = schema.NewSchemaHandlerFromSchemaList(sa)
		} else {
			if res.SchemaHandler, err = schema.NewSchemaHandlerFromStruct(obj); err != nil {
				return res, err
			}

			res.ObjType = reflect.TypeOf(obj).Elem()
		}
	} else {
		res.SchemaHandler = schema.NewSchemaHandlerFromSchemaList(res.Footer.Schema)
	}

	// res.RenameSchema() // Stop from renaming the schema
	for _, rowGroup := range res.Footer.RowGroups {
		for _, chunk := range rowGroup.Columns {
			exPath := make([]string, 0)
			exPath = append(exPath, res.SchemaHandler.GetRootExName())
			exPath = append(exPath, chunk.MetaData.GetPathInSchema()...)
			exPathStr := common.PathToStr(exPath)

			inPathStr := res.SchemaHandler.ExPathToInPath[exPathStr]
			inPath := common.StrToPath(inPathStr)[1:]
			chunk.MetaData.PathInSchema = inPath
		}
	}

	for i := 0; i < len(res.SchemaHandler.SchemaElements); i++ {
		schema := res.SchemaHandler.SchemaElements[i]
		if schema.GetNumChildren() == 0 {
			pathStr := res.SchemaHandler.IndexMap[int32(i)]
			if res.ColumnBuffers[pathStr], err = reader.NewColumnBuffer(pFile, res.Footer, res.SchemaHandler, pathStr); err != nil {
				return res, err
			}
		}
	}

	return res, nil
}

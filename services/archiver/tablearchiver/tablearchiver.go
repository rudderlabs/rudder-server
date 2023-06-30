package tablearchiver

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"text/template"
	"text/template/parse"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	PaginationAction = "{{.Pagination}}"
	OffsetAction     = "{{.Offset}}"
)

type TableJSONArchiver struct {
	DbHandle      *sql.DB
	Pagination    int
	Offset        int
	QueryTemplate string
	OutputPath    string
	FileManager   filemanager.FileManager
}

var pkgLogger logger.Logger

func init() {
	pkgLogger = logger.NewLogger().Child("tablearchiver")
}

func (jsonArchiver *TableJSONArchiver) Do() (location string, err error) {
	err = os.MkdirAll(filepath.Dir(jsonArchiver.OutputPath), os.ModePerm)
	if err != nil {
		pkgLogger.Errorf(`[TableJSONArchiver]: Error in creating local directory: %v`, err)
		return location, err
	}

	gzWriter, err := misc.CreateGZ(jsonArchiver.OutputPath)
	if err != nil {
		pkgLogger.Errorf(`[TableJSONArchiver]: Error in creating gzWriter: %v`, err)
		return location, err
	}

	t := template.Must(template.New("").Parse(jsonArchiver.QueryTemplate))
	var hasPagination bool
	for _, node := range t.Root.Nodes {
		if node.Type() == parse.NodeAction {
			if node.String() == PaginationAction {
				hasPagination = true
			}
		}
	}

	if hasPagination && jsonArchiver.Pagination < 1 {
		err = errors.New(`[TableJSONArchiver] Pagination limit is mandatory if query template has PaginationAction`)
		return location, err
	}

	offset := jsonArchiver.Offset
	for {
		data := map[string]int{
			"Pagination": jsonArchiver.Pagination,
			"Offset":     offset,
		}

		buf := bytes.Buffer{}
		if err := t.Execute(&buf, data); err != nil {
			return location, err
		}
		query := buf.String()

		var rawJSONRows sql.NullString
		row := jsonArchiver.DbHandle.QueryRow(query)
		err = row.Scan(&rawJSONRows)
		if err != nil {
			pkgLogger.Errorf(`[TableJSONArchiver]: Scanning row failed with error : %v`, err)
			return location, err
		}

		// break when json is null
		if !rawJSONRows.Valid {
			break
		}

		jsonBytes := []byte(rawJSONRows.String)

		jsonBytes = bytes.ReplaceAll(jsonBytes, []byte("}, \n {"), []byte("}\n{")) // replacing ", \n " with "\n"
		jsonBytes = jsonBytes[1 : len(jsonBytes)-1]                                // stripping starting '[' and ending ']'
		jsonBytes = append(jsonBytes, '\n')                                        // appending '\n'

		if _, err = gzWriter.Write(jsonBytes); err != nil {
			return location, err
		}

		if !hasPagination {
			break
		}

		offset += jsonArchiver.Pagination
	}

	if err = gzWriter.CloseGZ(); err != nil {
		return location, err
	}

	file, err := os.Open(jsonArchiver.OutputPath)
	if err != nil {
		pkgLogger.Errorf(`[TableJSONArchiver]: Error opening local file dump: %v`, err)
		return
	}
	defer func() { _ = file.Close() }()

	output, err := jsonArchiver.FileManager.Upload(context.TODO(), file)
	if err != nil {
		pkgLogger.Errorf(`[TableJSONArchiver]: Error uploading local file dump to object storage: %v`, err)
		return
	}
	location = output.Location
	return location, nil
}

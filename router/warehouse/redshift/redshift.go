package redshift

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/iancoleman/strcase"
	"github.com/rudderlabs/rudder-server/config"
	warehouseutils "github.com/rudderlabs/rudder-server/router/warehouse/utils"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/utils/misc"
	uuid "github.com/satori/go.uuid"
)

var (
	warehouseUploadsTable string
)

type HandleT struct {
	DbHandle      *sql.DB
	Db            *sql.DB
	UploadSchema  map[string]map[string]string
	CurrentSchema map[string]map[string]string
	UploadID      int64
	Warehouse     warehouseutils.WarehouseT
	SchemaName    string
	StartCSVID    int64
	EndCSVID      int64
}

var dataTypesMap = map[string]string{
	"boolean":  "boolean",
	"int":      "int",
	"float":    "double precision",
	"string":   "varchar(512)",
	"datetime": "timestamp",
}

func columnsWithDataTypes(columns map[string]string, prefix string) string {
	arr := []string{}
	for name, dataType := range columns {
		arr = append(arr, fmt.Sprintf(`%s%s %s`, prefix, name, dataTypesMap[dataType]))
	}
	return strings.Join(arr[:], ",")
}

func (rs *HandleT) setUploadError(err error) {
	warehouseutils.SetUploadStatus(rs.UploadID, warehouseutils.ExportingDataFailedState, rs.DbHandle)
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, error=$2 WHERE id=$3`, warehouseUploadsTable)
	_, err = rs.DbHandle.Exec(sqlStatement, warehouseutils.ExportingDataFailedState, err.Error(), rs.UploadID)
	misc.AssertError(err)
}

func (rs *HandleT) createTable(name string, columns map[string]string) (err error) {
	sortKeyField := "received_at"
	sqlStatement := fmt.Sprintf(`CREATE TABLE %s ( %v ) SORTKEY(%s)`, name, columnsWithDataTypes(columns, ""), sortKeyField)
	_, err = rs.Db.Exec(sqlStatement)
	return
}

func (rs *HandleT) addColumns(tableName string, columns map[string]string) (err error) {
	_, err = rs.Db.Exec(fmt.Sprintf(`ALTER TABLE %v  %v `, tableName, columnsWithDataTypes(columns, "ADD COLUMN ")))
	return
}

func (rs *HandleT) createSchema() (err error) {
	// TODO: Change to use source_schema_name in wh_schemas table
	_, err = rs.Db.Exec(fmt.Sprintf(`CREATE SCHEMA %s`, rs.SchemaName))
	return
}

func (rs *HandleT) updateSchema() (updatedSchema map[string]map[string]string, err error) {
	diff := warehouseutils.GetSchemaDiff(rs.CurrentSchema, rs.UploadSchema)
	updatedSchema = diff.UpdatedSchema
	if len(rs.CurrentSchema) == 0 {
		err = rs.createSchema()
		misc.AssertError(err)
	}
	processedTables := make(map[string]bool)
	for _, tableName := range diff.Tables {
		err = rs.createTable(fmt.Sprintf(`%s.%s`, rs.SchemaName, tableName), diff.ColumnMaps[tableName])
		misc.AssertError(err)
		processedTables[tableName] = true
	}
	for tableName, columnMap := range diff.ColumnMaps {
		if _, ok := processedTables[tableName]; ok {
			continue
		}
		if len(columnMap) > 0 {
			rs.addColumns(fmt.Sprintf(`%s.%s`, rs.SchemaName, tableName), columnMap)
		}
	}
	return
}

type S3ManifestEntryT struct {
	Url       string `json:"url"`
	Mandatory bool   `json:"mandatory"`
}

type S3ManifestT struct {
	Entries []S3ManifestEntryT `json:"entries"`
}

func (rs *HandleT) generateManifest(tableName string, columnMap map[string]string) (string, error) {
	csvObjectLocations, err := warehouseutils.GetCSVLocations(rs.DbHandle, tableName, rs.StartCSVID, rs.EndCSVID)
	misc.AssertError(err)
	csvS3Locations, err := warehouseutils.GetS3Locations(csvObjectLocations)
	var manifest S3ManifestT
	for _, location := range csvS3Locations {
		manifest.Entries = append(manifest.Entries, S3ManifestEntryT{Url: location, Mandatory: true})
	}
	manifestJSON, err := json.Marshal(&manifest)
	localManifestPath := fmt.Sprintf("%v%v%v", config.GetEnv("S3_UPLOADS_DIR", "/home/ubuntu/s3/"), "redshift-manifests/", uuid.NewV4().String())
	err = os.MkdirAll(filepath.Dir(localManifestPath), os.ModePerm)
	misc.AssertError(err)
	_ = ioutil.WriteFile(localManifestPath, manifestJSON, 0644)

	file, err := os.Open(localManifestPath)
	misc.AssertError(err)
	defer file.Close()

	uploader, err := fileuploader.NewFileUploader(&fileuploader.SettingsT{
		Provider:       "s3",
		AmazonS3Bucket: config.GetEnv("REDSHIFT_MANIFESTS_BUCKET", "rl-redshift-manifests"),
	})
	uploadLocation, err := uploader.Upload(file, rs.Warehouse.Source.ID, rs.Warehouse.Destination.ID, time.Now().Format("01-02-2006"), tableName, uuid.NewV4().String())

	misc.AssertError(err)

	uploadLocation = warehouseutils.GetS3Location(uploadLocation)

	return uploadLocation, nil
}

func (rs *HandleT) load(schema map[string]map[string]string) (err error) {
	for tableName, columnMap := range schema {
		manifestLocation, err := rs.generateManifest(tableName, columnMap)
		misc.AssertError(err)

		// sort columnnames
		keys := reflect.ValueOf(columnMap).MapKeys()
		strkeys := make([]string, len(keys))
		for i := 0; i < len(keys); i++ {
			strkeys[i] = keys[i].String()
		}
		sort.Strings(strkeys)
		sortedColumnNames := strings.Join(strkeys, ",")

		stagingTableName := "staging-" + uuid.NewV4().String()
		err = rs.createTable(fmt.Sprintf(`%s."%s"`, rs.SchemaName, stagingTableName), schema[tableName])
		if err != nil {
			rs.setUploadError(err)
			return err
		}

		// BEGIN TRANSACTION
		_, err = rs.Db.Exec("BEGIN TRANSACTION")
		if err != nil {
			rs.setUploadError(err)
			return err
		}

		sqlStatement := fmt.Sprintf(`COPY %v(%v) FROM '%v' CSV GZIP ACCESS_KEY_ID '%s' SECRET_ACCESS_KEY '%s' REGION 'us-east-1'  DATEFORMAT 'auto' TIMEFORMAT 'auto' MANIFEST TRUNCATECOLUMNS EMPTYASNULL FILLRECORD ACCEPTANYDATE TRIMBLANKS ACCEPTINVCHARS COMPUPDATE OFF `, fmt.Sprintf(`%s."%s"`, rs.SchemaName, stagingTableName), sortedColumnNames, manifestLocation, config.GetEnv("IAM_REDSHIFT_COPY_ACCESS_KEY_ID", ""), config.GetEnv("IAM_REDSHIFT_COPY_SECRET_ACCESS_KEY", ""))

		_, err = rs.Db.Exec(sqlStatement)
		if err != nil {
			rs.setUploadError(err)
			return err
		}

		sqlStatement = fmt.Sprintf(`delete from %[1]s."%[2]s" using %[1]s."%[3]s" _source where _source.id = %[1]s.%[2]s.id`, rs.SchemaName, tableName, stagingTableName)
		_, err = rs.Db.Exec(sqlStatement)
		if err != nil {
			rs.setUploadError(err)
			return err
		}

		var quotedColumnNames string
		for idx, str := range strkeys {
			quotedColumnNames += "\"" + str + "\""
			if idx != len(strkeys)-1 {
				quotedColumnNames += ","
			}
		}

		sqlStatement = fmt.Sprintf(`INSERT INTO %[1]s."%[2]s" (%[3]s) SELECT %[3]s FROM ( SELECT *, row_number() OVER (PARTITION BY id ORDER BY received_at ASC) AS _rudder_staging_row_number FROM %[1]s."%[4]s" ) AS _ where _rudder_staging_row_number = 1`, rs.SchemaName, tableName, quotedColumnNames, stagingTableName)
		_, err = rs.Db.Exec(sqlStatement)
		if err != nil {
			rs.setUploadError(err)
			return err
		}

		// END TRANSACTION
		_, err = rs.Db.Exec("END TRANSACTION")
		if err != nil {
			rs.setUploadError(err)
			return err
		}

		_, err = rs.Db.Exec(fmt.Sprintf(`DROP TABLE %[1]s."%[2]s"`, rs.SchemaName, stagingTableName))
		if err != nil {
			rs.setUploadError(err)
			return err
		}

	}
	return
}

// RedshiftCredentialsT ...
type RedshiftCredentialsT struct {
	host     string
	port     string
	dbName   string
	username string
	password string
}

func connect(cred RedshiftCredentialsT) (*sql.DB, error) {
	url := fmt.Sprintf("sslmode=require user=%v password=%v host=%v port=%v dbname=%v",
		cred.username,
		cred.password,
		cred.host,
		cred.port,
		cred.dbName)

	var err error
	var db *sql.DB
	if db, err = sql.Open("postgres", url); err != nil {
		return nil, fmt.Errorf("redshift connect error : (%v)", err)
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("redshift ping error : (%v)", err)
	}
	return db, nil
}

func loadConfig() {
	warehouseUploadsTable = config.GetString("Warehouse.uploadsTable", "wh_uploads")
}

func init() {
	config.Initialize()
	loadConfig()
}

func (rs *HandleT) Setup(config warehouseutils.ConfigT) {
	var err error
	rs.DbHandle = config.DbHandle
	rs.UploadID = config.UploadID
	rs.UploadSchema = config.Schema
	rs.Warehouse = config.Warehouse
	rs.StartCSVID = config.StartCSVID
	rs.EndCSVID = config.EndCSVID
	rs.Db, err = connect(RedshiftCredentialsT{
		host:     rs.Warehouse.Destination.Config.(map[string]interface{})["host"].(string),
		port:     rs.Warehouse.Destination.Config.(map[string]interface{})["port"].(string),
		dbName:   rs.Warehouse.Destination.Config.(map[string]interface{})["database"].(string),
		username: rs.Warehouse.Destination.Config.(map[string]interface{})["user"].(string),
		password: rs.Warehouse.Destination.Config.(map[string]interface{})["password"].(string),
	})
	if err != nil {
		rs.setUploadError(err)
		return
	}
	rs.CurrentSchema, err = warehouseutils.GetCurrentSchema(rs.DbHandle, rs.Warehouse)
	misc.AssertError(err)
	rs.SchemaName = strings.ToLower(strcase.ToSnake(rs.Warehouse.Source.Name))
	warehouseutils.SetUploadStatus(rs.UploadID, warehouseutils.UpdatingSchemaState, rs.DbHandle)
	updatedSchema, err := rs.updateSchema()
	if err != nil {
		rs.setUploadError(err)
		return
	}
	err = warehouseutils.SetUploadStatus(rs.UploadID, warehouseutils.UpdatedSchemaState, rs.DbHandle)
	misc.AssertError(err)
	err = warehouseutils.UpdateCurrentSchema(rs.Warehouse, rs.UploadID, rs.CurrentSchema, updatedSchema, rs.DbHandle)
	if err != nil {
		rs.setUploadError(err)
		return
	}
	err = warehouseutils.SetUploadStatus(rs.UploadID, warehouseutils.ExportingDataState, rs.DbHandle)
	misc.AssertError(err)
	err = rs.load(rs.UploadSchema)
	if err != nil {
		rs.setUploadError(err)
		return
	}
	err = warehouseutils.SetUploadStatus(rs.UploadID, warehouseutils.ExportedDataState, rs.DbHandle)
	misc.AssertError(err)
}

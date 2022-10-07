//go:build !warehouse_integration

package warehouse

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

func TestPickupStagingFileBucket(t *testing.T) {
	inputs := []struct {
		job      *PayloadT
		expected bool
	}{
		{
			job:      &PayloadT{},
			expected: false,
		},
		{
			job: &PayloadT{
				StagingDestinationRevisionID: "1liYatjkkCEVkEMYUmSWOE9eZ4n",
				DestinationRevisionID:        "1liYatjkkCEVkEMYUmSWOE9eZ4n",
			},
			expected: false,
		},
		{
			job: &PayloadT{
				StagingDestinationRevisionID: "1liYatjkkCEVkEMYUmSWOE9eZ4n",
				DestinationRevisionID:        "2liYatjkkCEVkEMYUmSWOE9eZ4n",
			},
			expected: false,
		},
		{
			job: &PayloadT{
				StagingDestinationRevisionID: "1liYatjkkCEVkEMYUmSWOE9eZ4n",
				DestinationRevisionID:        "2liYatjkkCEVkEMYUmSWOE9eZ4n",
				StagingDestinationConfig:     map[string]string{},
			},
			expected: true,
		},
	}
	for _, input := range inputs {
		got := PickupStagingConfiguration(input.job)
		require.Equal(t, got, input.expected)
	}
}

type TestFileManagerFactory struct {
	stagingFileLocation string
}

type TestFileManager struct {
	filemanager.FileManager
	stagingFileLocation string
}

func (fm *TestFileManager) Download(ctx context.Context, file *os.File, location string) error {
	r, err := os.Open(fm.stagingFileLocation)
	if err != nil {
		return fmt.Errorf("unable to open staging file for copying, err: %s", err.Error())
	}
	_, err = io.Copy(file, r)
	return err
}

// Upload simply captures the intent to upload the file to object storage. This is a no - op and can return an empty output
// to the caller,
func (fm *TestFileManager) Upload(ctx context.Context, file *os.File, keys ...string) (filemanager.UploadOutput, error) {
	return filemanager.UploadOutput{}, nil
}

func (factory *TestFileManagerFactory) New(settings *filemanager.SettingsT) (filemanager.FileManager, error) {
	return &TestFileManager{
		stagingFileLocation: factory.stagingFileLocation,
	}, nil
}

func TestStagingFileCreatesLoadObjects(t *testing.T) {
	setup()
	t.Setenv("RUDDER_TMPDIR", "testdata") // use the testdata local directory.
	// TODO: Write mainly implementation for the filemanager
	// which will be only looking for downloading a specific implementation for the staging file.
	job := PayloadT{
		BatchID:                      "ff139f07-5948-4220-a019-0e660a05cda5",
		UploadID:                     1,
		StagingFileID:                1,
		StagingFileLocation:          "staging_file.json.gz",
		UploadSchema:                 map[string]map[string]string{"rudder_discards": {"column_name": "string", "column_value": "string", "received_at": "datetime", "row_id": "string", "table_name": "string", "uuid_ts": "datetime"}, "test_track": {"_timestamp": "datetime", "anonymous_id": "string", "category": "string", "channel": "string", "context_app_build": "string", "context_app_name": "string", "context_app_namespace": "string", "context_app_version": "string", "context_destination_id": "string", "context_destination_type": "string", "context_device_id": "string", "context_device_manufacturer": "string", "context_device_model": "string", "context_device_name": "string", "context_ip": "string", "context_library_name": "string", "context_locale": "string", "context_network_carrier": "string", "context_request_ip": "string", "context_screen_density": "int", "context_screen_height": "int", "context_screen_width": "int", "context_source_id": "string", "context_source_type": "string", "context_traits_anonymous_id": "string", "context_user_agent": "string", "event": "string", "event_text": "string", "float_val": "float", "id": "string", "label": "string", "original_timestamp": "datetime", "received_at": "datetime", "sent_at": "datetime", "test_array": "string", "test_map_t_1_str": "string", "test_map_t_2_int": "int", "test_map_t_3_bool": "boolean", "uuid_ts": "datetime", "value": "int"}, "tracks": {"_timestamp": "datetime", "anonymous_id": "string", "channel": "string", "context_app_build": "string", "context_app_name": "string", "context_app_namespace": "string", "context_app_version": "string", "context_destination_id": "string", "context_destination_type": "string", "context_device_id": "string", "context_device_manufacturer": "string", "context_device_model": "string", "context_device_name": "string", "context_ip": "string", "context_library_name": "string", "context_locale": "string", "context_network_carrier": "string", "context_request_ip": "string", "context_screen_density": "int", "context_screen_height": "int", "context_screen_width": "int", "context_source_id": "string", "context_source_type": "string", "context_traits_anonymous_id": "string", "context_user_agent": "string", "event": "string", "event_text": "string", "id": "string", "original_timestamp": "datetime", "received_at": "datetime", "sent_at": "datetime", "uuid_ts": "datetime"}},
		SourceID:                     "26PluxPPBJ3sCWw4un7iJTm1VkE",
		SourceName:                   "dummy rudder source",
		DestinationID:                "26Q1xNtdz2ADK76UpNBiMw09PLo",
		DestinationName:              "s3-datalake-01",
		DestinationType:              "S3_DATALAKE",
		DestinationNamespace:         "rudder-schema",
		DestinationRevisionID:        "",
		StagingDestinationRevisionID: "",
		Destination:                  backendconfig.DestinationT{},
		StagingDestinationConfig:     map[string]interface{}{},
		UseRudderStorage:             false,
		StagingUseRudderStorage:      false,
		UniqueLoadGenID:              "",
		RudderStoragePrefix:          "none",
		LoadFilePrefix:               "",
		LoadFileType:                 "parquet",
		FileManagerFactory: &TestFileManagerFactory{
			stagingFileLocation: "testdata/slave/staging_file.json.gz",
		},
	}

	jobRun := JobRunT{
		job:                  &job,
		stagingFileDIR:       "slave/download",
		outputFileWritersMap: make(map[string]warehouseutils.LoadFileWriterI),
		eventLoaderTableMap:  make(map[string]warehouseutils.EventLoader),
		uuidTS:               time.Date(2022, 9, 8, 8, 25, 30, 0, time.UTC),
		tableEventCountMap:   make(map[string]int),
		whIdentifier: warehouseutils.GetWarehouseIdentifier(
			job.DestinationType,
			job.SourceID,
			job.DestinationID),
	}

	defer jobRun.cleanup()
	loadObjects, err := processStagingFile(context.TODO(), &job, &jobRun, 0)

	require.Nil(t, err)
	require.Equal(t, 2, len(loadObjects), "mismatch in actual vs expected load objects created")

	same, err := compareLoadFiles(
		fmt.Sprintf("testdata/slave/download/_0/%s_%s", job.DestinationType, job.DestinationID),
		map[string]string{
			"test_track": "testdata/slave/test_track.parquet",
			"tracks":     "testdata/slave/tracks.parquet",
		})

	require.Nil(t, err)
	require.True(t, same)
}

func compareLoadFiles(baseDir string, inputFiles map[string]string) (bool, error) {
	generatedFiles, err := ioutil.ReadDir(baseDir)
	if err != nil {
		return false, err
	}

	for tableName, expectedFile := range inputFiles {
		match := false

		for _, actualFile := range generatedFiles {

			if !strings.Contains(actualFile.Name(), tableName) {
				continue
			}

			match = true
			same, err := sameFile(baseDir+"/"+actualFile.Name(), expectedFile)
			if err != nil {
				return same, fmt.Errorf("unable to check same files for 1: %s, 2: %s, err: %w", actualFile.Name(), expectedFile, err)
			}
			if !same {
				return false, nil
			}
		}

		if !match {
			return false, fmt.Errorf("tableName: %s didn't match any actual files", tableName)
		}
	}
	return true, nil
}

func sameFile(loc1, loc2 string) (bool, error) {
	byt1, err := ioutil.ReadFile(loc1)
	if err != nil {
		return false, fmt.Errorf("unable to read complete file: %s, err: %w", loc1, err)
	}

	byt2, err := ioutil.ReadFile(loc2)
	if err != nil {
		return false, fmt.Errorf("unable to read complete file: %s, err: %w", loc2, err)
	}

	return bytes.Equal(byt1, byt2), nil
}

func setup() {
	config.Reset()
	logger.Reset()
	Init4()
	Init()
	misc.Init()
	warehouseutils.Init()
}

func BenchmarkEventLoaders(b *testing.B) {
	setup()
	b.Setenv("RUDDER_TMPDIR", "testdata") // use the testdata local directory.
	// TODO: Write mainly implementation for the filemanager
	// which will be only looking for downloading a specific implementation for the staging file.
	job := PayloadT{
		BatchID:                      "ff139f07-5948-4220-a019-0e660a05cda5",
		UploadID:                     1,
		StagingFileID:                1,
		StagingFileLocation:          "rudder-warehouse-staging-logs/26PluxPPBJ3sCWw4un7iJTm1VkE/2022-08-23/1661253062.26PluxPPBJ3sCWw4un7iJTm1VkE.181b4f7b-cc4c-48dd-88c0-a2cb3bb98544.json.gz",
		SourceID:                     "26PluxPPBJ3sCWw4un7iJTm1VkE",
		SourceName:                   "dummy rudder source",
		DestinationID:                "26Q1xNtdz2ADK76UpNBiMw09PLo",
		DestinationName:              "postgres-dev",
		DestinationType:              "S3_DATALAKE",
		DestinationNamespace:         "rudder-schema",
		DestinationRevisionID:        "",
		StagingDestinationRevisionID: "",
		Destination:                  backendconfig.DestinationT{},
		StagingDestinationConfig:     map[string]interface{}{},
		UseRudderStorage:             false,
		StagingUseRudderStorage:      false,
		UniqueLoadGenID:              "",
		RudderStoragePrefix:          "none",
		LoadFilePrefix:               "",
		LoadFileType:                 "parquet",
		FileManagerFactory:           &TestFileManagerFactory{stagingFileLocation: "testdata/slave/staging_file_large.json.gz"},
	}

	job.UploadSchema = map[string]map[string]string{"tracks": {"id": "string", "event": "string", "channel": "string", "sent_at": "datetime", "uuid_ts": "datetime", "_timestamp": "datetime", "context_ip": "string", "event_text": "string", "received_at": "datetime", "anonymous_id": "string", "context_source_id": "string", "context_request_ip": "string", "original_timestamp": "datetime", "context_source_type": "string", "context_destination_id": "string", "context_destination_type": "string"}, "event_loader_track": {"_0": "int", "_1": "int", "_2": "int", "_3": "int", "_4": "int", "_5": "int", "_6": "int", "_7": "int", "_8": "int", "_9": "int", "id": "string", "_10": "int", "_11": "int", "_12": "int", "_13": "int", "_14": "int", "_15": "int", "_16": "int", "_17": "int", "_18": "int", "_19": "int", "_20": "int", "_21": "int", "_22": "int", "_23": "int", "_24": "int", "_25": "int", "_26": "int", "_27": "int", "_28": "int", "_29": "int", "_30": "int", "_31": "int", "_32": "int", "_33": "int", "_34": "int", "_35": "int", "_36": "int", "_37": "int", "_38": "int", "_39": "int", "_40": "int", "_41": "int", "_42": "int", "_43": "int", "_44": "int", "_45": "int", "_46": "int", "_47": "int", "_48": "int", "_49": "int", "_50": "int", "_51": "int", "_52": "int", "_53": "int", "_54": "int", "_55": "int", "_56": "int", "_57": "int", "_58": "int", "_59": "int", "_60": "int", "_61": "int", "_62": "int", "_63": "int", "_64": "int", "_65": "int", "_66": "int", "_67": "int", "_68": "int", "_69": "int", "_70": "int", "_71": "int", "_72": "int", "_73": "int", "_74": "int", "_75": "int", "_76": "int", "_77": "int", "_78": "int", "_79": "int", "_80": "int", "_81": "int", "_82": "int", "_83": "int", "_84": "int", "_85": "int", "_86": "int", "_87": "int", "_88": "int", "_89": "int", "_90": "int", "_91": "int", "_92": "int", "_93": "int", "_94": "int", "_95": "int", "_96": "int", "_97": "int", "_98": "int", "_99": "int", "_100": "int", "_101": "int", "_102": "int", "_103": "int", "_104": "int", "_105": "int", "_106": "int", "_107": "int", "_108": "int", "_109": "int", "_110": "int", "_111": "int", "_112": "int", "_113": "int", "_114": "int", "_115": "int", "_116": "int", "_117": "int", "_118": "int", "_119": "int", "_120": "int", "_121": "int", "_122": "int", "_123": "int", "_124": "int", "_125": "int", "_126": "int", "_127": "int", "_128": "int", "_129": "int", "_130": "int", "_131": "int", "_132": "int", "_133": "int", "_134": "int", "_135": "int", "_136": "int", "_137": "int", "_138": "int", "_139": "int", "_140": "int", "_141": "int", "_142": "int", "_143": "int", "_144": "int", "_145": "int", "_146": "int", "_147": "int", "_148": "int", "_149": "int", "_150": "int", "_151": "int", "_152": "int", "_153": "int", "_154": "int", "_155": "int", "_156": "int", "_157": "int", "_158": "int", "_159": "int", "_160": "int", "_161": "int", "_162": "int", "_163": "int", "_164": "int", "_165": "int", "_166": "int", "_167": "int", "_168": "int", "_169": "int", "_170": "int", "_171": "int", "_172": "int", "_173": "int", "_174": "int", "_175": "int", "_176": "int", "_177": "int", "_178": "int", "_179": "int", "_180": "int", "_181": "int", "_182": "int", "_183": "int", "_184": "int", "_185": "int", "_186": "int", "_187": "int", "_188": "int", "_189": "int", "_190": "int", "_191": "int", "_192": "int", "_193": "int", "_194": "int", "_195": "int", "_196": "int", "_197": "int", "_198": "int", "_199": "int", "_200": "int", "_201": "int", "_202": "int", "_203": "int", "_204": "int", "_205": "int", "_206": "int", "_207": "int", "_208": "int", "_209": "int", "_210": "int", "_211": "int", "_212": "int", "_213": "int", "_214": "int", "_215": "int", "_216": "int", "_217": "int", "_218": "int", "_219": "int", "_220": "int", "_221": "int", "_222": "int", "_223": "int", "_224": "int", "_225": "int", "_226": "int", "_227": "int", "_228": "int", "_229": "int", "_230": "int", "_231": "int", "_232": "int", "_233": "int", "_234": "int", "_235": "int", "_236": "int", "_237": "int", "_238": "int", "_239": "int", "_240": "int", "_241": "int", "_242": "int", "_243": "int", "_244": "int", "_245": "int", "_246": "int", "_247": "int", "_248": "int", "_249": "int", "_250": "int", "_251": "int", "_252": "int", "_253": "int", "_254": "int", "_255": "int", "_256": "int", "_257": "int", "_258": "int", "_259": "int", "_260": "int", "_261": "int", "_262": "int", "_263": "int", "_264": "int", "_265": "int", "_266": "int", "_267": "int", "_268": "int", "_269": "int", "_270": "int", "_271": "int", "_272": "int", "_273": "int", "_274": "int", "_275": "int", "_276": "int", "_277": "int", "_278": "int", "_279": "int", "_280": "int", "_281": "int", "_282": "int", "_283": "int", "_284": "int", "_285": "int", "_286": "int", "_287": "int", "_288": "int", "_289": "int", "_290": "int", "_291": "int", "_292": "int", "_293": "int", "_294": "int", "_295": "int", "_296": "int", "_297": "int", "_298": "int", "_299": "int", "_300": "int", "_301": "int", "_302": "int", "_303": "int", "_304": "int", "_305": "int", "_306": "int", "_307": "int", "_308": "int", "_309": "int", "_310": "int", "_311": "int", "_312": "int", "_313": "int", "_314": "int", "_315": "int", "_316": "int", "_317": "int", "_318": "int", "_319": "int", "_320": "int", "_321": "int", "_322": "int", "_323": "int", "_324": "int", "_325": "int", "_326": "int", "_327": "int", "_328": "int", "_329": "int", "_330": "int", "_331": "int", "_332": "int", "_333": "int", "_334": "int", "_335": "int", "_336": "int", "_337": "int", "_338": "int", "_339": "int", "_340": "int", "_341": "int", "_342": "int", "_343": "int", "_344": "int", "_345": "int", "_346": "int", "_347": "int", "_348": "int", "_349": "int", "_350": "int", "_351": "int", "_352": "int", "_353": "int", "_354": "int", "_355": "int", "_356": "int", "_357": "int", "_358": "int", "_359": "int", "_360": "int", "_361": "int", "_362": "int", "_363": "int", "_364": "int", "_365": "int", "_366": "int", "_367": "int", "_368": "int", "_369": "int", "_370": "int", "_371": "int", "_372": "int", "_373": "int", "_374": "int", "_375": "int", "_376": "int", "_377": "int", "_378": "int", "_379": "int", "_380": "int", "_381": "int", "_382": "int", "_383": "int", "_384": "int", "_385": "int", "_386": "int", "_387": "int", "_388": "int", "_389": "int", "_390": "int", "_391": "int", "_392": "int", "_393": "int", "_394": "int", "_395": "int", "_396": "int", "_397": "int", "_398": "int", "_399": "int", "_400": "int", "_401": "int", "_402": "int", "_403": "int", "_404": "int", "_405": "int", "_406": "int", "_407": "int", "_408": "int", "_409": "int", "_410": "int", "_411": "int", "_412": "int", "_413": "int", "_414": "int", "_415": "int", "_416": "int", "_417": "int", "_418": "int", "_419": "int", "_420": "int", "_421": "int", "_422": "int", "_423": "int", "_424": "int", "_425": "int", "_426": "int", "_427": "int", "_428": "int", "_429": "int", "_430": "int", "_431": "int", "_432": "int", "_433": "int", "_434": "int", "_435": "int", "_436": "int", "_437": "int", "_438": "int", "_439": "int", "_440": "int", "_441": "int", "_442": "int", "_443": "int", "_444": "int", "_445": "int", "_446": "int", "_447": "int", "_448": "int", "_449": "int", "_450": "int", "_451": "int", "_452": "int", "_453": "int", "_454": "int", "_455": "int", "_456": "int", "_457": "int", "_458": "int", "_459": "int", "_460": "int", "_461": "int", "_462": "int", "_463": "int", "_464": "int", "_465": "int", "_466": "int", "_467": "int", "_468": "int", "_469": "int", "_470": "int", "_471": "int", "_472": "int", "_473": "int", "_474": "int", "_475": "int", "_476": "int", "_477": "int", "_478": "int", "_479": "int", "_480": "int", "_481": "int", "_482": "int", "_483": "int", "_484": "int", "_485": "int", "_486": "int", "_487": "int", "_488": "int", "_489": "int", "_490": "int", "_491": "int", "_492": "int", "_493": "int", "_494": "int", "_495": "int", "_496": "int", "_497": "int", "_498": "int", "_499": "int", "_500": "int", "_501": "int", "_502": "int", "_503": "int", "_504": "int", "_505": "int", "_506": "int", "_507": "int", "_508": "int", "_509": "int", "_510": "int", "_511": "int", "_512": "int", "_513": "int", "_514": "int", "_515": "int", "_516": "int", "_517": "int", "_518": "int", "_519": "int", "_520": "int", "_521": "int", "_522": "int", "_523": "int", "_524": "int", "_525": "int", "_526": "int", "_527": "int", "_528": "int", "_529": "int", "_530": "int", "_531": "int", "_532": "int", "_533": "int", "_534": "int", "_535": "int", "_536": "int", "_537": "int", "_538": "int", "_539": "int", "_540": "int", "_541": "int", "_542": "int", "_543": "int", "_544": "int", "_545": "int", "_546": "int", "_547": "int", "_548": "int", "_549": "int", "_550": "int", "_551": "int", "_552": "int", "_553": "int", "_554": "int", "_555": "int", "_556": "int", "_557": "int", "_558": "int", "_559": "int", "_560": "int", "_561": "int", "_562": "int", "_563": "int", "_564": "int", "_565": "int", "_566": "int", "_567": "int", "_568": "int", "_569": "int", "_570": "int", "_571": "int", "_572": "int", "_573": "int", "_574": "int", "_575": "int", "_576": "int", "_577": "int", "_578": "int", "_579": "int", "_580": "int", "_581": "int", "_582": "int", "_583": "int", "_584": "int", "_585": "int", "_586": "int", "_587": "int", "_588": "int", "_589": "int", "_590": "int", "_591": "int", "_592": "int", "_593": "int", "_594": "int", "_595": "int", "_596": "int", "_597": "int", "_598": "int", "_599": "int", "_600": "int", "_601": "int", "_602": "int", "_603": "int", "_604": "int", "_605": "int", "_606": "int", "_607": "int", "_608": "int", "_609": "int", "_610": "int", "_611": "int", "_612": "int", "_613": "int", "_614": "int", "_615": "int", "_616": "int", "_617": "int", "_618": "int", "_619": "int", "_620": "int", "_621": "int", "_622": "int", "_623": "int", "_624": "int", "_625": "int", "_626": "int", "_627": "int", "_628": "int", "_629": "int", "_630": "int", "_631": "int", "_632": "int", "_633": "int", "_634": "int", "_635": "int", "_636": "int", "_637": "int", "_638": "int", "_639": "int", "_640": "int", "_641": "int", "_642": "int", "_643": "int", "_644": "int", "_645": "int", "_646": "int", "_647": "int", "_648": "int", "_649": "int", "_650": "int", "_651": "int", "_652": "int", "_653": "int", "_654": "int", "_655": "int", "_656": "int", "_657": "int", "_658": "int", "_659": "int", "_660": "int", "_661": "int", "_662": "int", "_663": "int", "_664": "int", "_665": "int", "_666": "int", "_667": "int", "_668": "int", "_669": "int", "_670": "int", "_671": "int", "_672": "int", "_673": "int", "_674": "int", "_675": "int", "_676": "int", "_677": "int", "_678": "int", "_679": "int", "_680": "int", "_681": "int", "_682": "int", "_683": "int", "_684": "int", "_685": "int", "_686": "int", "_687": "int", "_688": "int", "_689": "int", "_690": "int", "_691": "int", "_692": "int", "_693": "int", "_694": "int", "_695": "int", "_696": "int", "_697": "int", "_698": "int", "_699": "int", "_700": "int", "_701": "int", "_702": "int", "_703": "int", "_704": "int", "_705": "int", "_706": "int", "_707": "int", "_708": "int", "_709": "int", "_710": "int", "_711": "int", "_712": "int", "_713": "int", "_714": "int", "_715": "int", "_716": "int", "_717": "int", "_718": "int", "_719": "int", "_720": "int", "_721": "int", "_722": "int", "_723": "int", "_724": "int", "_725": "int", "_726": "int", "_727": "int", "_728": "int", "_729": "int", "_730": "int", "_731": "int", "_732": "int", "_733": "int", "_734": "int", "_735": "int", "_736": "int", "_737": "int", "_738": "int", "_739": "int", "_740": "int", "_741": "int", "_742": "int", "_743": "int", "_744": "int", "_745": "int", "_746": "int", "_747": "int", "_748": "int", "_749": "int", "_750": "int", "_751": "int", "_752": "int", "_753": "int", "_754": "int", "_755": "int", "_756": "int", "_757": "int", "_758": "int", "_759": "int", "_760": "int", "_761": "int", "_762": "int", "_763": "int", "_764": "int", "_765": "int", "_766": "int", "_767": "int", "_768": "int", "_769": "int", "_770": "int", "_771": "int", "_772": "int", "_773": "int", "_774": "int", "_775": "int", "_776": "int", "_777": "int", "_778": "int", "_779": "int", "_780": "int", "_781": "int", "_782": "int", "_783": "int", "_784": "int", "_785": "int", "_786": "int", "_787": "int", "_788": "int", "_789": "int", "_790": "int", "_791": "int", "_792": "int", "_793": "int", "_794": "int", "_795": "int", "_796": "int", "_797": "int", "_798": "int", "_799": "int", "_800": "int", "_801": "int", "_802": "int", "_803": "int", "_804": "int", "_805": "int", "_806": "int", "_807": "int", "_808": "int", "_809": "int", "_810": "int", "_811": "int", "_812": "int", "_813": "int", "_814": "int", "_815": "int", "_816": "int", "_817": "int", "_818": "int", "_819": "int", "_820": "int", "_821": "int", "_822": "int", "_823": "int", "_824": "int", "_825": "int", "_826": "int", "_827": "int", "_828": "int", "_829": "int", "_830": "int", "_831": "int", "_832": "int", "_833": "int", "_834": "int", "_835": "int", "_836": "int", "_837": "int", "_838": "int", "_839": "int", "_840": "int", "_841": "int", "_842": "int", "_843": "int", "_844": "int", "_845": "int", "_846": "int", "_847": "int", "_848": "int", "_849": "int", "_850": "int", "_851": "int", "_852": "int", "_853": "int", "_854": "int", "_855": "int", "_856": "int", "_857": "int", "_858": "int", "_859": "int", "_860": "int", "_861": "int", "_862": "int", "_863": "int", "_864": "int", "_865": "int", "_866": "int", "_867": "int", "_868": "int", "_869": "int", "_870": "int", "_871": "int", "_872": "int", "_873": "int", "_874": "int", "_875": "int", "_876": "int", "_877": "int", "_878": "int", "_879": "int", "_880": "int", "_881": "int", "_882": "int", "_883": "int", "_884": "int", "_885": "int", "_886": "int", "_887": "int", "_888": "int", "_889": "int", "_890": "int", "_891": "int", "_892": "int", "_893": "int", "_894": "int", "_895": "int", "_896": "int", "_897": "int", "_898": "int", "_899": "int", "_900": "int", "_901": "int", "_902": "int", "_903": "int", "_904": "int", "_905": "int", "_906": "int", "_907": "int", "_908": "int", "_909": "int", "_910": "int", "_911": "int", "_912": "int", "_913": "int", "_914": "int", "_915": "int", "_916": "int", "_917": "int", "_918": "int", "_919": "int", "_920": "int", "_921": "int", "_922": "int", "_923": "int", "_924": "int", "_925": "int", "_926": "int", "_927": "int", "_928": "int", "_929": "int", "_930": "int", "_931": "int", "_932": "int", "_933": "int", "_934": "int", "_935": "int", "_936": "int", "_937": "int", "_938": "int", "_939": "int", "_940": "int", "_941": "int", "_942": "int", "_943": "int", "_944": "int", "_945": "int", "_946": "int", "_947": "int", "_948": "int", "_949": "int", "_950": "int", "_951": "int", "_952": "int", "_953": "int", "_954": "int", "_955": "int", "_956": "int", "_957": "int", "_958": "int", "_959": "int", "_960": "int", "_961": "int", "_962": "int", "_963": "int", "_964": "int", "_965": "int", "_966": "int", "_967": "int", "_968": "int", "_969": "int", "_970": "int", "_971": "int", "_972": "int", "_973": "int", "_974": "int", "_975": "int", "_976": "int", "_977": "int", "_978": "int", "_979": "int", "_980": "int", "_981": "int", "_982": "int", "_983": "int", "_984": "int", "_985": "int", "_986": "int", "_987": "int", "_988": "int", "_989": "int", "_990": "int", "_991": "int", "_992": "int", "_993": "int", "_994": "int", "_995": "int", "_996": "int", "_997": "int", "_998": "int", "_999": "int", "event": "string", "channel": "string", "sent_at": "datetime", "uuid_ts": "datetime", "_timestamp": "datetime", "context_ip": "string", "event_text": "string", "received_at": "datetime", "anonymous_id": "string", "context_source_id": "string", "context_request_ip": "string", "original_timestamp": "datetime", "context_source_type": "string", "context_destination_id": "string", "context_destination_type": "string"}}

	b.ResetTimer()

	var jobRun *JobRunT
	for i := 0; i < b.N; i++ {
		jobRun = &JobRunT{
			job:                  &job,
			stagingFileDIR:       "slave/download",
			outputFileWritersMap: make(map[string]warehouseutils.LoadFileWriterI),
			eventLoaderTableMap:  make(map[string]warehouseutils.EventLoader),
			uuidTS:               time.Now(),
			tableEventCountMap:   make(map[string]int),
			whIdentifier: warehouseutils.GetWarehouseIdentifier(
				job.DestinationType,
				job.SourceID,
				job.DestinationID),
		}
		_, err := processStagingFile(context.TODO(), &job, jobRun, 0)
		if err != nil {
			b.Fatalf("err not nil, err: %s", err.Error())
		}
		jobRun.cleanup()
	}
}

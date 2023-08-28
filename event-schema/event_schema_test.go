package event_schema

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/jeremywohl/flatten"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-server/admin"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/testhelper"
)

type envSetter interface {
	Setenv(key, value string)
}

func setupDB(es envSetter, cleanup *testhelper.Cleanup) (*resource.PostgresResource, error) {
	log.Println("Initialize the database here with the necessary table structures.")

	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, fmt.Errorf("Unable to bring up pool for creating containers, err: %w", err)
	}

	pgResource, err := resource.SetupPostgres(pool, cleanup)
	if err != nil {
		return nil, fmt.Errorf("Unable to setup the postgres container, err: %w", err)
	}

	mg := &migrator.Migrator{
		Handle:                     pgResource.DB,
		MigrationsTable:            "node_migrations",
		ShouldForceSetLowerVersion: config.GetBool("SQLMigrator.forceSetLowerVersion", true),
	}

	err = mg.Migrate("node")
	if err != nil {
		return nil, fmt.Errorf("Unable to run the migrations for the node, err: %w", err)
	}

	// Load self configuration.
	config.Reset()

	logger.Reset() // Initialize the logger
	Init2()
	Init()

	// Setup the supporting services like jobsdb
	jobsDBInit(es, pgResource)

	return pgResource, nil
}

func jobsDBInit(es envSetter, pgResource *resource.PostgresResource) {
	// Setup env for jobsdb.
	es.Setenv("JOBS_DB_HOST", pgResource.Host)
	es.Setenv("JOBS_DB_USER", pgResource.User)
	es.Setenv("JOBS_DB_PASSWORD", pgResource.Password)
	es.Setenv("JOBS_DB_DB_NAME", pgResource.Database)
	es.Setenv("JOBS_DB_PORT", pgResource.Port)

	admin.Init()
}

var _ = Describe("Event Schemas", Ordered, func() {
	var pgResource *resource.PostgresResource
	var err error
	cleanup := &testhelper.Cleanup{}

	BeforeAll(func() {
		pgResource, err = setupDB(GinkgoT(), cleanup)

		if err != nil {
			Fail(fmt.Sprintf("unable to setup the postgres resource: %s", err.Error()))
		}
	})

	Describe("Event Schema Lifecycle", func() {
		Context("when new event is seen", func() {
			It("generates new event schema in db", func() {
				writeKey := "my-write-key"
				manager := getEventSchemaManager(
					pgResource.DB, false)
				eventStr := `{"batch": [{"type": "track", "event": "Demo Track", "sentAt": "2019-08-12T05:08:30.909Z", "channel": "android-sdk", "context": {"app": {"name": "RudderAndroidClient", "build": "1", "version": "1.0", "namespace": "com.rudderlabs.android.sdk"}, "device": {"id": "49e4bdd1c280bc00", "name": "generic_x86", "model": "Android SDK built for x86", "manufacturer": "Google"}, "locale": "en-US", "screen": {"width": 1080, "height": 1794, "density": 420}, "traits": {"anonymousId": "49e4bdd1c280bc00"}, "library": {"name": "com.rudderstack.android.sdk.core"}, "network": {"carrier": "Android"}, "user_agent": "Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"}, "rudderId": "90ca6da0-292e-4e79-9880-f8009e0ae4a3", "messageId": "a82717d0-b939-47bd-9592-59bbbea66c1a", "properties": {"label": "Demo Label", "value": 5, "testMap": {"t1": "a", "t2": 4}, "category": "Demo Category", "floatVal": 4.501, "testArray": [{"id": "elem1", "value": "e1"}, {"id": "elem2", "value": "e2"}]}, "anonymousId": "anon_id", "integrations": {"All": true}, "originalTimestamp": "2019-08-12T05:08:30.909Z"}], "writeKey": "my-write-key", "requestIP": "127.0.0.1", "receivedAt": "2022-06-15T13:51:08.754+05:30"}`
				var eventPayload EventPayloadT
				err := json.Unmarshal([]byte(eventStr), &eventPayload)
				Expect(err).To(BeNil(), "Invalid request payload for unmarshalling")

				// Send event across a new write key
				manager.handleEvent(writeKey, eventPayload.Batch[0])

				// Event Model gets stored in the in-memory map
				eventModel := manager.eventModelMap[WriteKey(writeKey)][("track")][("Demo Track")]
				Expect(eventModel.UUID).ToNot(BeEmpty())
				Expect(eventModel.EventIdentifier).To(BeEquivalentTo("Demo Track"))

				// Schema Version gets stored in the in-memory map for event model.
				eventMap := map[string]interface{}(eventPayload.Batch[0])
				flattenedEvent, _ := flatten.Flatten(eventMap, "", flatten.DotStyle)
				hash := getSchemaHash(getSchema(flattenedEvent))

				schemaVersion := manager.schemaVersionMap[eventModel.UUID][hash]
				Expect(schemaVersion).ToNot(BeNil())
			})
		})
	})

	Describe("Event Schema Frequency Counters", func() {
		Context("when frequency counters are limited", func() {
			BeforeEach(func() {
				frequencyCounterLimit = 200
			})

			It("trims frequency counters for event model reading from db", func() {
				writeKey := "my-write-key"

				manager := getEventSchemaManager(pgResource.DB, false)
				eventStr := `{"batch": [{"type": "track", "event": "Demo Track", "properties": {"label": "Demo Label", "value": 5 }}], "writeKey": "my-write-key", "requestIP": "127.0.0.1", "receivedAt": "2022-06-15T13:51:08.754+05:30"}`
				var eventPayload EventPayloadT
				err := json.Unmarshal([]byte(eventStr), &eventPayload)
				Expect(err).To(BeNil())

				manager.handleEvent(writeKey, eventPayload.Batch[0])
				eventModel := manager.eventModelMap[WriteKey(writeKey)]["track"]["Demo Track"]

				// Push the events to DB
				manager.flushEventSchemasToDB(context.TODO())

				// Bound the frequency counters to 3
				frequencyCounterLimit = 3

				// reload the models from the database which should now respect
				// that frequency counters have now been bounded.
				manager.handleEvent(writeKey, eventPayload.Batch[0])
				Expect(len(countersCache[eventModel.UUID])).To(BeEquivalentTo(3))

				// flush the events back to the database.
				err = manager.flushEventSchemasToDB(context.TODO())
				Expect(err).To(BeNil())

				freqCounters, err := getFrequencyCountersForEventModel(manager.dbHandle, eventModel.UUID)
				Expect(err).To(BeNil())
				Expect(len(freqCounters)).To(BeEquivalentTo(3))
			})

			It("trims frequency counters for event model reading from in-memory", func() {
				writeKey := "my-write-key-in-memory"

				manager := getEventSchemaManager(pgResource.DB, false)
				eventStr := `{"batch": [{"type": "track", "event": "Demo Track", "properties": {"label": "Demo Label", "value": 5 }}], "writeKey": "my-write-key", "requestIP": "127.0.0.1", "receivedAt": "2022-06-15T13:51:08.754+05:30"}`
				var eventPayload EventPayloadT
				json.Unmarshal([]byte(eventStr), &eventPayload)

				manager.handleEvent(writeKey, eventPayload.Batch[0])
				eventModel := manager.eventModelMap[WriteKey(writeKey)]["track"]["Demo Track"]

				// Bound the frequency counters to 3
				frequencyCounterLimit = 3

				// When we receive another event for loaded in memory, we should
				// prune the frequency counters to the limit
				manager.handleEvent(writeKey, eventPayload.Batch[0])
				Expect(len(countersCache[eventModel.UUID])).To(BeEquivalentTo(frequencyCounterLimit))
			})
		})
	})

	AfterAll(func() {
		// clean up the resources.
		cleanup.Run()
	})
})

func getFrequencyCountersForEventModel(handle *sql.DB, uuid string) ([]*FrequencyCounter, error) {
	query := `SELECT private_data FROM event_models WHERE uuid = $1`
	var privateDataRaw json.RawMessage
	err := handle.QueryRow(query, uuid).Scan(&privateDataRaw)
	if err != nil {
		return nil, err
	}

	var privateData PrivateDataT
	err = json.Unmarshal(privateDataRaw, &privateData)
	if err != nil {
		return nil, err
	}

	return privateData.FrequencyCounters, nil
}

func TestPruneFrequencyCounters(t *testing.T) {
	inputs := []struct {
		hash              string
		counters          map[string]*FrequencyCounter
		bound             int
		countersRemaining int
	}{
		{
			"prune-schema-hash-1",
			map[string]*FrequencyCounter{
				"k1": {},
				"k2": {},
				"k3": {},
			},
			2, 2, // 1 extra element will be removed
		},
		{
			"prune-schema-hash-2",
			map[string]*FrequencyCounter{
				"k1": {},
				"k2": {},
			},
			0, 0, // all the entries will be removed as bound is 0
		},
		{
			"prune-schema-hash-2",
			map[string]*FrequencyCounter{
				"k1": {},
				"k2": {},
			},
			3, 2, // all the entries will be intact as bound > existing entries
		},
	}

	for _, input := range inputs {
		t.Logf("Pruning for event schema hash: %s", input.hash)
		countersCache[input.hash] = input.counters
		pruneFrequencyCounters(input.hash, input.bound)

		require.Equal(t, len(countersCache[input.hash]), input.countersRemaining)
	}
}

func BenchmarkEventSchemaHandleEvent(b *testing.B) {
	b.Log("Benchmarking the handling event of event schema")

	cleanup := &testhelper.Cleanup{}
	pgResource, err := setupDB(b, cleanup)
	if err != nil {
		b.Errorf(fmt.Sprintf("unable to setup db resource: %s", err.Error()))
		return
	}

	defer cleanup.Run()

	byt, err := os.ReadFile("testdata/test_input.json")
	if err != nil {
		b.Errorf("Unable to perform benchmark test as unable to read the input value")
		return
	}

	var eventPayload EventPayloadT
	if err := json.Unmarshal(byt, &eventPayload); err != nil {
		b.Errorf("Invalid request payload for unmarshalling: %v", err.Error())
		return
	}

	manager := EventSchemaManagerT{
		dbHandle:             pgResource.DB,
		disableInMemoryCache: false,
		eventModelMap:        EventModelMapT{},
		schemaVersionMap:     SchemaVersionMapT{},
	}

	// frequencyCounterLimit = ?
	for i := 0; i < b.N; i++ {
		manager.handleEvent("dummy-key", eventPayload.Batch[0])
	}

	// flush the event schemas to the database.
	err = manager.flushEventSchemasToDB(context.TODO())
	if err != nil {
		b.Errorf("Unable to flush events back to database")
	}
}

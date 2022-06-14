// Code generated by ObjectBox; DO NOT EDIT.
// Learn more about defining entities and generating this file - visit https://golang.objectbox.io/entity-annotations

package objectdb

import (
	"encoding/json"
	"errors"
	"github.com/google/flatbuffers/go"
	"github.com/objectbox/objectbox-go/objectbox"
	"github.com/objectbox/objectbox-go/objectbox/fbutils"
)

type gatewayJob_EntityInfo struct {
	objectbox.Entity
	Uid uint64
}

var GatewayJobBinding = gatewayJob_EntityInfo{
	Entity: objectbox.Entity{
		Id: 11,
	},
	Uid: 7535172036551034281,
}

// GatewayJob_ contains type-based Property helpers to facilitate some common operations such as Queries.
var GatewayJob_ = struct {
	JobID              *objectbox.PropertyUint64
	UserID             *objectbox.PropertyString
	WorkspaceID        *objectbox.PropertyString
	CreatedAt          *objectbox.PropertyInt64
	ExpireAt           *objectbox.PropertyInt64
	EventCount         *objectbox.PropertyInt
	EventPayload       *objectbox.PropertyByteVector
	PayloadSize        *objectbox.PropertyInt64
	ExecTime           *objectbox.PropertyInt64
	RetryTime          *objectbox.PropertyInt64
	ErrorResponse      *objectbox.PropertyByteVector
	SourceID           *objectbox.PropertyString
	SourceBatchID      *objectbox.PropertyString
	SourceTaskID       *objectbox.PropertyString
	SourceTaskRunID    *objectbox.PropertyString
	SourceJobID        *objectbox.PropertyString
	SourceJobRunID     *objectbox.PropertyString
	SourceDefinitionID *objectbox.PropertyString
	JobState           *objectbox.RelationToOne
}{
	JobID: &objectbox.PropertyUint64{
		BaseProperty: &objectbox.BaseProperty{
			Id:     1,
			Entity: &GatewayJobBinding.Entity,
		},
	},
	UserID: &objectbox.PropertyString{
		BaseProperty: &objectbox.BaseProperty{
			Id:     2,
			Entity: &GatewayJobBinding.Entity,
		},
	},
	WorkspaceID: &objectbox.PropertyString{
		BaseProperty: &objectbox.BaseProperty{
			Id:     4,
			Entity: &GatewayJobBinding.Entity,
		},
	},
	CreatedAt: &objectbox.PropertyInt64{
		BaseProperty: &objectbox.BaseProperty{
			Id:     5,
			Entity: &GatewayJobBinding.Entity,
		},
	},
	ExpireAt: &objectbox.PropertyInt64{
		BaseProperty: &objectbox.BaseProperty{
			Id:     6,
			Entity: &GatewayJobBinding.Entity,
		},
	},
	EventCount: &objectbox.PropertyInt{
		BaseProperty: &objectbox.BaseProperty{
			Id:     7,
			Entity: &GatewayJobBinding.Entity,
		},
	},
	EventPayload: &objectbox.PropertyByteVector{
		BaseProperty: &objectbox.BaseProperty{
			Id:     8,
			Entity: &GatewayJobBinding.Entity,
		},
	},
	PayloadSize: &objectbox.PropertyInt64{
		BaseProperty: &objectbox.BaseProperty{
			Id:     9,
			Entity: &GatewayJobBinding.Entity,
		},
	},
	ExecTime: &objectbox.PropertyInt64{
		BaseProperty: &objectbox.BaseProperty{
			Id:     10,
			Entity: &GatewayJobBinding.Entity,
		},
	},
	RetryTime: &objectbox.PropertyInt64{
		BaseProperty: &objectbox.BaseProperty{
			Id:     11,
			Entity: &GatewayJobBinding.Entity,
		},
	},
	ErrorResponse: &objectbox.PropertyByteVector{
		BaseProperty: &objectbox.BaseProperty{
			Id:     12,
			Entity: &GatewayJobBinding.Entity,
		},
	},
	SourceID: &objectbox.PropertyString{
		BaseProperty: &objectbox.BaseProperty{
			Id:     13,
			Entity: &GatewayJobBinding.Entity,
		},
	},
	SourceBatchID: &objectbox.PropertyString{
		BaseProperty: &objectbox.BaseProperty{
			Id:     14,
			Entity: &GatewayJobBinding.Entity,
		},
	},
	SourceTaskID: &objectbox.PropertyString{
		BaseProperty: &objectbox.BaseProperty{
			Id:     15,
			Entity: &GatewayJobBinding.Entity,
		},
	},
	SourceTaskRunID: &objectbox.PropertyString{
		BaseProperty: &objectbox.BaseProperty{
			Id:     16,
			Entity: &GatewayJobBinding.Entity,
		},
	},
	SourceJobID: &objectbox.PropertyString{
		BaseProperty: &objectbox.BaseProperty{
			Id:     17,
			Entity: &GatewayJobBinding.Entity,
		},
	},
	SourceJobRunID: &objectbox.PropertyString{
		BaseProperty: &objectbox.BaseProperty{
			Id:     18,
			Entity: &GatewayJobBinding.Entity,
		},
	},
	SourceDefinitionID: &objectbox.PropertyString{
		BaseProperty: &objectbox.BaseProperty{
			Id:     19,
			Entity: &GatewayJobBinding.Entity,
		},
	},
	JobState: &objectbox.RelationToOne{
		Property: &objectbox.BaseProperty{
			Id:     22,
			Entity: &GatewayJobBinding.Entity,
		},
		Target: &JobStateBinding.Entity,
	},
}

// GeneratorVersion is called by ObjectBox to verify the compatibility of the generator used to generate this code
func (gatewayJob_EntityInfo) GeneratorVersion() int {
	return 6
}

// AddToModel is called by ObjectBox during model build
func (gatewayJob_EntityInfo) AddToModel(model *objectbox.Model) {
	model.Entity("GatewayJob", 11, 7535172036551034281)
	model.Property("JobID", 6, 1, 5143458760441539061)
	model.PropertyFlags(1)
	model.Property("UserID", 9, 2, 8998124193677116215)
	model.Property("WorkspaceID", 9, 4, 4207858374913391455)
	model.Property("CreatedAt", 10, 5, 655876097213894740)
	model.Property("ExpireAt", 10, 6, 257177343896885499)
	model.Property("EventCount", 6, 7, 7524534071422409018)
	model.Property("EventPayload", 23, 8, 2323166441397103419)
	model.Property("PayloadSize", 6, 9, 5387083218644166782)
	model.Property("ExecTime", 10, 10, 4035491400602821353)
	model.Property("RetryTime", 10, 11, 8234322648838789589)
	model.Property("ErrorResponse", 23, 12, 2201414932138568655)
	model.Property("SourceID", 9, 13, 615597211562445870)
	model.Property("SourceBatchID", 9, 14, 1826099015080728190)
	model.Property("SourceTaskID", 9, 15, 6993815264733201885)
	model.Property("SourceTaskRunID", 9, 16, 7075223750815829428)
	model.Property("SourceJobID", 9, 17, 99980625877429802)
	model.Property("SourceJobRunID", 9, 18, 2497701676685710269)
	model.Property("SourceDefinitionID", 9, 19, 1472550507374473002)
	model.Property("JobState", 11, 22, 8762647828934847572)
	model.PropertyFlags(520)
	model.PropertyRelation("JobState", 32, 1840706778753093380)
	model.EntityLastPropertyId(22, 8762647828934847572)
}

// GetId is called by ObjectBox during Put operations to check for existing ID on an object
func (gatewayJob_EntityInfo) GetId(object interface{}) (uint64, error) {
	return object.(*GatewayJob).JobID, nil
}

// SetId is called by ObjectBox during Put to update an ID on an object that has just been inserted
func (gatewayJob_EntityInfo) SetId(object interface{}, id uint64) error {
	object.(*GatewayJob).JobID = id
	return nil
}

// PutRelated is called by ObjectBox to put related entities before the object itself is flattened and put
func (gatewayJob_EntityInfo) PutRelated(ob *objectbox.ObjectBox, object interface{}, id uint64) error {
	if rel := object.(*GatewayJob).JobState; rel != nil {
		if rId, err := JobStateBinding.GetId(rel); err != nil {
			return err
		} else if rId == 0 {
			// NOTE Put/PutAsync() has a side-effect of setting the rel.ID
			if _, err := BoxForJobState(ob).Put(rel); err != nil {
				return err
			}
		}
	}
	return nil
}

// Flatten is called by ObjectBox to transform an object to a FlatBuffer
func (gatewayJob_EntityInfo) Flatten(object interface{}, fbb *flatbuffers.Builder, id uint64) error {
	obj := object.(*GatewayJob)
	var propCreatedAt int64
	{
		var err error
		propCreatedAt, err = objectbox.TimeInt64ConvertToDatabaseValue(obj.CreatedAt)
		if err != nil {
			return errors.New("converter objectbox.TimeInt64ConvertToDatabaseValue() failed on GatewayJob.CreatedAt: " + err.Error())
		}
	}

	var propExpireAt int64
	{
		var err error
		propExpireAt, err = objectbox.TimeInt64ConvertToDatabaseValue(obj.ExpireAt)
		if err != nil {
			return errors.New("converter objectbox.TimeInt64ConvertToDatabaseValue() failed on GatewayJob.ExpireAt: " + err.Error())
		}
	}

	var propExecTime int64
	{
		var err error
		propExecTime, err = objectbox.TimeInt64ConvertToDatabaseValue(obj.ExecTime)
		if err != nil {
			return errors.New("converter objectbox.TimeInt64ConvertToDatabaseValue() failed on GatewayJob.ExecTime: " + err.Error())
		}
	}

	var propRetryTime int64
	{
		var err error
		propRetryTime, err = objectbox.TimeInt64ConvertToDatabaseValue(obj.RetryTime)
		if err != nil {
			return errors.New("converter objectbox.TimeInt64ConvertToDatabaseValue() failed on GatewayJob.RetryTime: " + err.Error())
		}
	}

	var offsetUserID = fbutils.CreateStringOffset(fbb, obj.UserID)
	var offsetWorkspaceID = fbutils.CreateStringOffset(fbb, obj.WorkspaceID)
	var offsetEventPayload = fbutils.CreateByteVectorOffset(fbb, []byte(obj.EventPayload))
	var offsetErrorResponse = fbutils.CreateByteVectorOffset(fbb, []byte(obj.ErrorResponse))
	var offsetSourceID = fbutils.CreateStringOffset(fbb, obj.SourceID)
	var offsetSourceBatchID = fbutils.CreateStringOffset(fbb, obj.SourceBatchID)
	var offsetSourceTaskID = fbutils.CreateStringOffset(fbb, obj.SourceTaskID)
	var offsetSourceTaskRunID = fbutils.CreateStringOffset(fbb, obj.SourceTaskRunID)
	var offsetSourceJobID = fbutils.CreateStringOffset(fbb, obj.SourceJobID)
	var offsetSourceJobRunID = fbutils.CreateStringOffset(fbb, obj.SourceJobRunID)
	var offsetSourceDefinitionID = fbutils.CreateStringOffset(fbb, obj.SourceDefinitionID)

	var rIdJobState uint64
	if rel := obj.JobState; rel != nil {
		if rId, err := JobStateBinding.GetId(rel); err != nil {
			return err
		} else {
			rIdJobState = rId
		}
	}

	// build the FlatBuffers object
	fbb.StartObject(22)
	fbutils.SetUint64Slot(fbb, 0, id)
	fbutils.SetUOffsetTSlot(fbb, 1, offsetUserID)
	if obj.JobState != nil {
		fbutils.SetUint64Slot(fbb, 21, rIdJobState)
	}
	fbutils.SetUOffsetTSlot(fbb, 3, offsetWorkspaceID)
	fbutils.SetInt64Slot(fbb, 4, propCreatedAt)
	fbutils.SetInt64Slot(fbb, 5, propExpireAt)
	fbutils.SetInt64Slot(fbb, 6, int64(obj.EventCount))
	fbutils.SetUOffsetTSlot(fbb, 7, offsetEventPayload)
	fbutils.SetInt64Slot(fbb, 8, obj.PayloadSize)
	fbutils.SetInt64Slot(fbb, 9, propExecTime)
	fbutils.SetInt64Slot(fbb, 10, propRetryTime)
	fbutils.SetUOffsetTSlot(fbb, 11, offsetErrorResponse)
	fbutils.SetUOffsetTSlot(fbb, 12, offsetSourceID)
	fbutils.SetUOffsetTSlot(fbb, 13, offsetSourceBatchID)
	fbutils.SetUOffsetTSlot(fbb, 14, offsetSourceTaskID)
	fbutils.SetUOffsetTSlot(fbb, 15, offsetSourceTaskRunID)
	fbutils.SetUOffsetTSlot(fbb, 16, offsetSourceJobID)
	fbutils.SetUOffsetTSlot(fbb, 17, offsetSourceJobRunID)
	fbutils.SetUOffsetTSlot(fbb, 18, offsetSourceDefinitionID)
	return nil
}

// Load is called by ObjectBox to load an object from a FlatBuffer
func (gatewayJob_EntityInfo) Load(ob *objectbox.ObjectBox, bytes []byte) (interface{}, error) {
	if len(bytes) == 0 { // sanity check, should "never" happen
		return nil, errors.New("can't deserialize an object of type 'GatewayJob' - no data received")
	}

	var table = &flatbuffers.Table{
		Bytes: bytes,
		Pos:   flatbuffers.GetUOffsetT(bytes),
	}

	var propJobID = table.GetUint64Slot(4, 0)

	propCreatedAt, err := objectbox.TimeInt64ConvertToEntityProperty(fbutils.GetInt64Slot(table, 12))
	if err != nil {
		return nil, errors.New("converter objectbox.TimeInt64ConvertToEntityProperty() failed on GatewayJob.CreatedAt: " + err.Error())
	}

	propExpireAt, err := objectbox.TimeInt64ConvertToEntityProperty(fbutils.GetInt64Slot(table, 14))
	if err != nil {
		return nil, errors.New("converter objectbox.TimeInt64ConvertToEntityProperty() failed on GatewayJob.ExpireAt: " + err.Error())
	}

	propExecTime, err := objectbox.TimeInt64ConvertToEntityProperty(fbutils.GetInt64Slot(table, 22))
	if err != nil {
		return nil, errors.New("converter objectbox.TimeInt64ConvertToEntityProperty() failed on GatewayJob.ExecTime: " + err.Error())
	}

	propRetryTime, err := objectbox.TimeInt64ConvertToEntityProperty(fbutils.GetInt64Slot(table, 24))
	if err != nil {
		return nil, errors.New("converter objectbox.TimeInt64ConvertToEntityProperty() failed on GatewayJob.RetryTime: " + err.Error())
	}

	var relJobState *JobState
	if rId := fbutils.GetUint64PtrSlot(table, 46); rId != nil && *rId > 0 {
		if rObject, err := BoxForJobState(ob).Get(*rId); err != nil {
			return nil, err
		} else {
			relJobState = rObject
		}
	}

	return &GatewayJob{
		JobID:              propJobID,
		UserID:             fbutils.GetStringSlot(table, 6),
		JobState:           relJobState,
		WorkspaceID:        fbutils.GetStringSlot(table, 10),
		CreatedAt:          propCreatedAt,
		ExpireAt:           propExpireAt,
		EventCount:         fbutils.GetIntSlot(table, 16),
		EventPayload:       json.RawMessage(fbutils.GetByteVectorSlot(table, 18)),
		PayloadSize:        fbutils.GetInt64Slot(table, 20),
		ExecTime:           propExecTime,
		RetryTime:          propRetryTime,
		ErrorResponse:      json.RawMessage(fbutils.GetByteVectorSlot(table, 26)),
		SourceID:           fbutils.GetStringSlot(table, 28),
		SourceBatchID:      fbutils.GetStringSlot(table, 30),
		SourceTaskID:       fbutils.GetStringSlot(table, 32),
		SourceTaskRunID:    fbutils.GetStringSlot(table, 34),
		SourceJobID:        fbutils.GetStringSlot(table, 36),
		SourceJobRunID:     fbutils.GetStringSlot(table, 38),
		SourceDefinitionID: fbutils.GetStringSlot(table, 40),
	}, nil
}

// MakeSlice is called by ObjectBox to construct a new slice to hold the read objects
func (gatewayJob_EntityInfo) MakeSlice(capacity int) interface{} {
	return make([]*GatewayJob, 0, capacity)
}

// AppendToSlice is called by ObjectBox to fill the slice of the read objects
func (gatewayJob_EntityInfo) AppendToSlice(slice interface{}, object interface{}) interface{} {
	if object == nil {
		return append(slice.([]*GatewayJob), nil)
	}
	return append(slice.([]*GatewayJob), object.(*GatewayJob))
}

// Box provides CRUD access to GatewayJob objects
type GatewayJobBox struct {
	*objectbox.Box
}

// BoxForGatewayJob opens a box of GatewayJob objects
func BoxForGatewayJob(ob *objectbox.ObjectBox) *GatewayJobBox {
	return &GatewayJobBox{
		Box: ob.InternalBox(11),
	}
}

// Put synchronously inserts/updates a single object.
// In case the JobID is not specified, it would be assigned automatically (auto-increment).
// When inserting, the GatewayJob.JobID property on the passed object will be assigned the new ID as well.
func (box *GatewayJobBox) Put(object *GatewayJob) (uint64, error) {
	return box.Box.Put(object)
}

// Insert synchronously inserts a single object. As opposed to Put, Insert will fail if given an ID that already exists.
// In case the JobID is not specified, it would be assigned automatically (auto-increment).
// When inserting, the GatewayJob.JobID property on the passed object will be assigned the new ID as well.
func (box *GatewayJobBox) Insert(object *GatewayJob) (uint64, error) {
	return box.Box.Insert(object)
}

// Update synchronously updates a single object.
// As opposed to Put, Update will fail if an object with the same ID is not found in the database.
func (box *GatewayJobBox) Update(object *GatewayJob) error {
	return box.Box.Update(object)
}

// PutAsync asynchronously inserts/updates a single object.
// Deprecated: use box.Async().Put() instead
func (box *GatewayJobBox) PutAsync(object *GatewayJob) (uint64, error) {
	return box.Box.PutAsync(object)
}

// PutMany inserts multiple objects in single transaction.
// In case JobIDs are not set on the objects, they would be assigned automatically (auto-increment).
//
// Returns: IDs of the put objects (in the same order).
// When inserting, the GatewayJob.JobID property on the objects in the slice will be assigned the new IDs as well.
//
// Note: In case an error occurs during the transaction, some of the objects may already have the GatewayJob.JobID assigned
// even though the transaction has been rolled back and the objects are not stored under those IDs.
//
// Note: The slice may be empty or even nil; in both cases, an empty IDs slice and no error is returned.
func (box *GatewayJobBox) PutMany(objects []*GatewayJob) ([]uint64, error) {
	return box.Box.PutMany(objects)
}

// Get reads a single object.
//
// Returns nil (and no error) in case the object with the given ID doesn't exist.
func (box *GatewayJobBox) Get(id uint64) (*GatewayJob, error) {
	object, err := box.Box.Get(id)
	if err != nil {
		return nil, err
	} else if object == nil {
		return nil, nil
	}
	return object.(*GatewayJob), nil
}

// GetMany reads multiple objects at once.
// If any of the objects doesn't exist, its position in the return slice is nil
func (box *GatewayJobBox) GetMany(ids ...uint64) ([]*GatewayJob, error) {
	objects, err := box.Box.GetMany(ids...)
	if err != nil {
		return nil, err
	}
	return objects.([]*GatewayJob), nil
}

// GetManyExisting reads multiple objects at once, skipping those that do not exist.
func (box *GatewayJobBox) GetManyExisting(ids ...uint64) ([]*GatewayJob, error) {
	objects, err := box.Box.GetManyExisting(ids...)
	if err != nil {
		return nil, err
	}
	return objects.([]*GatewayJob), nil
}

// GetAll reads all stored objects
func (box *GatewayJobBox) GetAll() ([]*GatewayJob, error) {
	objects, err := box.Box.GetAll()
	if err != nil {
		return nil, err
	}
	return objects.([]*GatewayJob), nil
}

// Remove deletes a single object
func (box *GatewayJobBox) Remove(object *GatewayJob) error {
	return box.Box.Remove(object)
}

// RemoveMany deletes multiple objects at once.
// Returns the number of deleted object or error on failure.
// Note that this method will not fail if an object is not found (e.g. already removed).
// In case you need to strictly check whether all of the objects exist before removing them,
// you can execute multiple box.Contains() and box.Remove() inside a single write transaction.
func (box *GatewayJobBox) RemoveMany(objects ...*GatewayJob) (uint64, error) {
	var ids = make([]uint64, len(objects))
	for k, object := range objects {
		ids[k] = object.JobID
	}
	return box.Box.RemoveIds(ids...)
}

// Creates a query with the given conditions. Use the fields of the GatewayJob_ struct to create conditions.
// Keep the *GatewayJobQuery if you intend to execute the query multiple times.
// Note: this function panics if you try to create illegal queries; e.g. use properties of an alien type.
// This is typically a programming error. Use QueryOrError instead if you want the explicit error check.
func (box *GatewayJobBox) Query(conditions ...objectbox.Condition) *GatewayJobQuery {
	return &GatewayJobQuery{
		box.Box.Query(conditions...),
	}
}

// Creates a query with the given conditions. Use the fields of the GatewayJob_ struct to create conditions.
// Keep the *GatewayJobQuery if you intend to execute the query multiple times.
func (box *GatewayJobBox) QueryOrError(conditions ...objectbox.Condition) (*GatewayJobQuery, error) {
	if query, err := box.Box.QueryOrError(conditions...); err != nil {
		return nil, err
	} else {
		return &GatewayJobQuery{query}, nil
	}
}

// Async provides access to the default Async Box for asynchronous operations. See GatewayJobAsyncBox for more information.
func (box *GatewayJobBox) Async() *GatewayJobAsyncBox {
	return &GatewayJobAsyncBox{AsyncBox: box.Box.Async()}
}

// GatewayJobAsyncBox provides asynchronous operations on GatewayJob objects.
//
// Asynchronous operations are executed on a separate internal thread for better performance.
//
// There are two main use cases:
//
// 1) "execute & forget:" you gain faster put/remove operations as you don't have to wait for the transaction to finish.
//
// 2) Many small transactions: if your write load is typically a lot of individual puts that happen in parallel,
// this will merge small transactions into bigger ones. This results in a significant gain in overall throughput.
//
// In situations with (extremely) high async load, an async method may be throttled (~1ms) or delayed up to 1 second.
// In the unlikely event that the object could still not be enqueued (full queue), an error will be returned.
//
// Note that async methods do not give you hard durability guarantees like the synchronous Box provides.
// There is a small time window in which the data may not have been committed durably yet.
type GatewayJobAsyncBox struct {
	*objectbox.AsyncBox
}

// AsyncBoxForGatewayJob creates a new async box with the given operation timeout in case an async queue is full.
// The returned struct must be freed explicitly using the Close() method.
// It's usually preferable to use GatewayJobBox::Async() which takes care of resource management and doesn't require closing.
func AsyncBoxForGatewayJob(ob *objectbox.ObjectBox, timeoutMs uint64) *GatewayJobAsyncBox {
	var async, err = objectbox.NewAsyncBox(ob, 11, timeoutMs)
	if err != nil {
		panic("Could not create async box for entity ID 11: %s" + err.Error())
	}
	return &GatewayJobAsyncBox{AsyncBox: async}
}

// Put inserts/updates a single object asynchronously.
// When inserting a new object, the JobID property on the passed object will be assigned the new ID the entity would hold
// if the insert is ultimately successful. The newly assigned ID may not become valid if the insert fails.
func (asyncBox *GatewayJobAsyncBox) Put(object *GatewayJob) (uint64, error) {
	return asyncBox.AsyncBox.Put(object)
}

// Insert a single object asynchronously.
// The JobID property on the passed object will be assigned the new ID the entity would hold if the insert is ultimately
// successful. The newly assigned ID may not become valid if the insert fails.
// Fails silently if an object with the same ID already exists (this error is not returned).
func (asyncBox *GatewayJobAsyncBox) Insert(object *GatewayJob) (id uint64, err error) {
	return asyncBox.AsyncBox.Insert(object)
}

// Update a single object asynchronously.
// The object must already exists or the update fails silently (without an error returned).
func (asyncBox *GatewayJobAsyncBox) Update(object *GatewayJob) error {
	return asyncBox.AsyncBox.Update(object)
}

// Remove deletes a single object asynchronously.
func (asyncBox *GatewayJobAsyncBox) Remove(object *GatewayJob) error {
	return asyncBox.AsyncBox.Remove(object)
}

// Query provides a way to search stored objects
//
// For example, you can find all GatewayJob which JobID is either 42 or 47:
// 		box.Query(GatewayJob_.JobID.In(42, 47)).Find()
type GatewayJobQuery struct {
	*objectbox.Query
}

// Find returns all objects matching the query
func (query *GatewayJobQuery) Find() ([]*GatewayJob, error) {
	objects, err := query.Query.Find()
	if err != nil {
		return nil, err
	}
	return objects.([]*GatewayJob), nil
}

// Offset defines the index of the first object to process (how many objects to skip)
func (query *GatewayJobQuery) Offset(offset uint64) *GatewayJobQuery {
	query.Query.Offset(offset)
	return query
}

// Limit sets the number of elements to process by the query
func (query *GatewayJobQuery) Limit(limit uint64) *GatewayJobQuery {
	query.Query.Limit(limit)
	return query
}

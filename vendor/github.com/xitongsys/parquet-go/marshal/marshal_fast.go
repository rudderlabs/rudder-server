package marshal

import (
	"errors"
	"fmt"
	"github.com/goccy/go-reflect"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/types"
	"unsafe"
)

// MarshalFast implements the Marshal function while maximizing performance and minimizing
// allocations.
//
// For each type it attempts to marshal, it uses reflection to compile an encoder. Then,
// for each element, it looks up the encoder for the type and encodes it by walking the
// pointers of the structure, appending leaf elements into their respective tables.
//
// This function makes liberal use of the unsafe package to avoid allocations when
// performing normal reflection. It also uses the "github.com/goccy/go-reflect" package (a
// drop-in alternative to the normal "reflect" package) to make use of some additional
// features. (Namely TypeID and TypeAndPtrOf.)
//
// It does not support map-type fields. It should support every other use-case of Marshal.
func MarshalFast(srcInterface []interface{}, schemaHandler *schema.SchemaHandler) (tb *map[string]*layout.Table, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("unkown error")
			}
		}
	}()

	tableMap := setupTableMap(schemaHandler, len(srcInterface))
	pathMap := schemaHandler.PathMap

	c := compiler{
		encoderMap:    make(map[encoderMapKey]encoder),
		tableMap:      tableMap,
		schemaHandler: schemaHandler,
	}

	for _, v := range srcInterface {
		typ, ptr := reflect.TypeAndPtrOf(v)
		enc := c.getEncoder(typ, pathMap)
		enc.encode(ptr, 0, 0)
	}

	return &tableMap, nil
}

type encoder interface {
	encode(ptr unsafe.Pointer, dl, rl int32)
}

// encodeMapKey is the key used to lookup compiled encoders.
//
// It uses both the typeID and the pathMap because the encoder stores the table that it
// targets, which is different for each path, and because the same path may have multiple
// encoders for different types, such as for pointers or interfaces and for the underlying
// value.
type encoderMapKey struct {
	typeID  uintptr
	pathMap *schema.PathMapType
}

func (k encoderMapKey) String() string {
	return fmt.Sprintf("{%d, %s}", k.typeID, k.pathMap.Path)
}

// compiler handles compiling encoders and caching them so that they can be reused across
// multiple objects.
//
// Currently, the caching is local to the compiler, which requires re-compiling on every
// call to MarshalFast. We could cache across calls to MarshalFast, but this is
// complicated by the need to synchronize across multiple goroutines, and re-compiling
// doesn't comprise a significant portion of execution time when writing large files.
type compiler struct {
	encoderMap    map[encoderMapKey]encoder
	tableMap      map[string]*layout.Table
	schemaHandler *schema.SchemaHandler
}

// getEncoder returns an encoder for the type and path provided. It looks up a cached
// encoder if one exists, otherwise compiles a new one and caches it.
func (c *compiler) getEncoder(typ reflect.Type, pathMap *schema.PathMapType) encoder {
	var enc encoder
	var ok bool
	typeIface := reflect.Zero(typ).Interface()
	typeID := reflect.TypeID(typeIface)
	key := encoderMapKey{typeID, pathMap}
	if enc, ok = c.encoderMap[key]; !ok {
		enc = c.compile(typ, pathMap)
		c.encoderMap[key] = enc
	}
	return enc
}

func (c *compiler) compile(typ reflect.Type, pathMap *schema.PathMapType) encoder {
	switch typ.Kind() {
	case reflect.Ptr:
		return c.compilePointer(typ, pathMap)
	case reflect.Interface:
		return c.compileInterface(typ, pathMap)
	case reflect.Struct:
		return c.compileStruct(typ, pathMap)
	case reflect.Slice:
		return c.compileSlice(typ, pathMap)
	case reflect.Map:
		panic("FastMarshal does not support map fields.")
	default:
		te := c.terminalEncoder(typ, pathMap)
		return &te
	}
}

func (c *compiler) getSchema(path string) *parquet.SchemaElement {
	schemaIndex := c.schemaHandler.MapIndex[path]
	return c.schemaHandler.SchemaElements[schemaIndex]
}

func (c *compiler) terminalEncoder(typ reflect.Type, pathMap *schema.PathMapType) terminalEncoder {
	typeIface := reflect.Zero(typ).Interface()
	path := pathMap.Path
	return terminalEncoder{
		typeIface: typeIface,
		table:     c.tableMap[path],
		pT:        c.getSchema(path).Type,
	}
}

func (c *compiler) compilePointer(typ reflect.Type, pathMap *schema.PathMapType) encoder {
	var valEncoder encoder
	valEncoder = c.getEncoder(typ.Elem(), pathMap)
	return &pointerEncoder{valEncoder}
}

func (c *compiler) compileInterface(typ reflect.Type, pathMap *schema.PathMapType) encoder {
	var childTables []*layout.Table
	for tablePath, table := range c.tableMap {
		if common.IsChildPath(pathMap.Path, tablePath) {
			childTables = append(childTables, table)
		}
	}

	return &ifaceEncoder{c, pathMap, childTables}
}

func (c *compiler) compileStruct(typ reflect.Type, pathMap *schema.PathMapType) encoder {
	if len(pathMap.Children) == 0 {
		return &nilEncoder{c.terminalEncoder(typ, pathMap)}
	}
	numFields := typ.NumField()
	encoders := make([]structFieldEncoder, 0, numFields)
	for i := 0; i < numFields; i++ {
		tf := typ.Field(i)
		name := tf.Name
		if fPathMap, ok := pathMap.Children[name]; ok {
			encoders = append(encoders, structFieldEncoder{
				offset: tf.Offset,
				enc:    c.getEncoder(tf.Type, fPathMap),
			})
		}
	}
	return &structEncoder{encoders}
}

func (c *compiler) compileSlice(typ reflect.Type, pathMap *schema.PathMapType) encoder {

	if *c.getSchema(pathMap.Path).RepetitionType != parquet.FieldRepetitionType_REPEATED {
		pathMap = pathMap.Children["List"].Children["Element"]
	}

	valEncoder := c.getEncoder(typ.Elem(), pathMap)
	elemSize := typ.Elem().Size()
	return &sliceEncoder{elemSize, valEncoder}
}

// emptyInterface shares the structure underlying an interface{}. It allows us to
// build one on the stack without performing an allocation.
type emptyInterface struct {
	typ unsafe.Pointer
	val unsafe.Pointer
}

// toIface converts a typeIface and an unsafe.Pointer into an empty interface with the
// same type as typeIface and referencing the value of unsafe.Pointer without performing
// an allocation.
// This is safe only if the ptr is heap-allocated. However, we can infer that the value is
// heap-allocated because the code traversed to ptr from the root object, which came from
// a []interface{} in the input to MarshalFast.
func toIface(typeIface interface{}, ptr unsafe.Pointer) interface{} {
	ei := emptyInterface{
		typ: *(*unsafe.Pointer)(unsafe.Pointer(&typeIface)),
		val: ptr,
	}
	return *(*interface{})(unsafe.Pointer(&ei))
}

// terminalEncoder handles encoding of terminal (leaf) values that don't have children.
type terminalEncoder struct {
	typeIface interface{}
	table     *layout.Table
	pT        *parquet.Type
}

// encode converts a pointer back to an interface of the correct type and appends it
// to the table with definition-level and repetition-level.
func (e *terminalEncoder) encode(ptr unsafe.Pointer, dl, rl int32) {
	var v interface{}
	if ptr != nil {
		v = toIface(e.typeIface, ptr)
	}
	e.write(v, dl, rl)
}

func (e *terminalEncoder) write(v interface{}, dl, rl int32) {
	e.table.Values = append(e.table.Values, types.InterfaceToParquetType(v, e.pT))
	e.table.DefinitionLevels = append(e.table.DefinitionLevels, dl)
	e.table.RepetitionLevels = append(e.table.RepetitionLevels, rl)
}

// nilEncoder handles encoding of values known to be nil.
type nilEncoder struct {
	terminalEncoder
}

func (e *nilEncoder) encode(ptr unsafe.Pointer, dl, rl int32) {
	e.write(nil, dl, rl)
}

type structFieldEncoder struct {
	offset uintptr
	enc    encoder
}

// structEncoder handles encoding of struct types. For each field to be encoded, it
// increments the pointer by the offset of the field and delegates to an appropriate
// encoder.
type structEncoder struct {
	fieldEncoders []structFieldEncoder
}

func (e *structEncoder) encode(ptr unsafe.Pointer, dl, rl int32) {
	for _, fe := range e.fieldEncoders {
		var fPtr unsafe.Pointer
		if ptr != nil {
			fPtr = unsafe.Pointer(uintptr(ptr) + fe.offset)
		}
		fe.enc.encode(fPtr, dl, rl)
	}
}

// pointerEncoder handles encoding of pointer types. If dereferences the pointer and
// delegates to an appropriate encoder.
type pointerEncoder struct {
	valEncoder encoder
}

func (e *pointerEncoder) encode(ptr unsafe.Pointer, dl, rl int32) {
	if ptr != nil {
		ptr = *(*unsafe.Pointer)(ptr)
	}
	if ptr != nil {
		dl++
	}
	e.valEncoder.encode(ptr, dl, rl)
}

// sliceEncoder handles encoding of slice types. It finds the length and address of the
// first element, then increments the pointer by element offsets and delegates to an
// appropriate encoder.
type sliceEncoder struct {
	elemSize   uintptr
	valEncoder encoder
}

func (e *sliceEncoder) encode(ptr unsafe.Pointer, dl, rl int32) {
	var sliceHeader []interface{}
	if ptr != nil {
		sliceHeader = *(*[]interface{})(ptr)
	}
	if len(sliceHeader) == 0 {
		e.valEncoder.encode(nil, dl, rl)
		return
	}
	headPtr := unsafe.Pointer(&sliceHeader[0])
	for i := 0; i < len(sliceHeader); i++ {
		elemPtr := unsafe.Pointer(uintptr(headPtr) + uintptr(i)*e.elemSize)
		e.valEncoder.encode(elemPtr, dl+1, rl)
		if i == 0 {
			rl++
		}
	}
}

// ifaceEncoder handles encoding of interface types. It dynamically looks up an encoder
// for the interface type. If the type is nil, it encodes nils in all child tables.
// This dynamic lookup is necessary because the value is an interface and therefore could
// be of multiple different types, which we don't know until we encode the individual
// value.
type ifaceEncoder struct {
	c       *compiler
	pathMap *schema.PathMapType
	tables  []*layout.Table
}

func (e *ifaceEncoder) encode(ptr unsafe.Pointer, dl, rl int32) {
	iface := *(*interface{})(ptr)
	typ, subPtr := reflect.TypeAndPtrOf(iface)
	if typ == nil {
		term := terminalEncoder{}
		for _, t := range e.tables {
			term.table = t
			term.encode(nil, dl, rl)
		}
	} else {
		enc := e.c.getEncoder(typ, e.pathMap)
		enc.encode(subPtr, dl, rl)
	}
}

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.21.12
// source: proto/event-schema/types.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type EventSchemaKey struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	WriteKey        string `protobuf:"bytes,1,opt,name=writeKey,proto3" json:"writeKey,omitempty"`
	EventType       string `protobuf:"bytes,2,opt,name=eventType,proto3" json:"eventType,omitempty"`
	EventIdentifier string `protobuf:"bytes,3,opt,name=eventIdentifier,proto3" json:"eventIdentifier,omitempty"`
}

func (x *EventSchemaKey) Reset() {
	*x = EventSchemaKey{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_event_schema_types_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *EventSchemaKey) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*EventSchemaKey) ProtoMessage() {}

func (x *EventSchemaKey) ProtoReflect() protoreflect.Message {
	mi := &file_proto_event_schema_types_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use EventSchemaKey.ProtoReflect.Descriptor instead.
func (*EventSchemaKey) Descriptor() ([]byte, []int) {
	return file_proto_event_schema_types_proto_rawDescGZIP(), []int{0}
}

func (x *EventSchemaKey) GetWriteKey() string {
	if x != nil {
		return x.WriteKey
	}
	return ""
}

func (x *EventSchemaKey) GetEventType() string {
	if x != nil {
		return x.EventType
	}
	return ""
}

func (x *EventSchemaKey) GetEventIdentifier() string {
	if x != nil {
		return x.EventIdentifier
	}
	return ""
}

type EventSchemaMessage struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	WorkspaceID   string                 `protobuf:"bytes,1,opt,name=workspaceID,proto3" json:"workspaceID,omitempty"`
	Key           *EventSchemaKey        `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	Schema        map[string]string      `protobuf:"bytes,3,rep,name=schema,proto3" json:"schema,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	ObservedAt    *timestamppb.Timestamp `protobuf:"bytes,4,opt,name=observedAt,proto3" json:"observedAt,omitempty"`
	Sample        []byte                 `protobuf:"bytes,5,opt,name=sample,proto3" json:"sample,omitempty"`
	CorrelationID []byte                 `protobuf:"bytes,6,opt,name=correlationID,proto3" json:"correlationID,omitempty"`
	Hash          string                 `protobuf:"bytes,7,opt,name=hash,proto3" json:"hash,omitempty"`
	BatchCount    int64                  `protobuf:"varint,8,opt,name=batchCount,proto3" json:"batchCount,omitempty"`
}

func (x *EventSchemaMessage) Reset() {
	*x = EventSchemaMessage{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_event_schema_types_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *EventSchemaMessage) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*EventSchemaMessage) ProtoMessage() {}

func (x *EventSchemaMessage) ProtoReflect() protoreflect.Message {
	mi := &file_proto_event_schema_types_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use EventSchemaMessage.ProtoReflect.Descriptor instead.
func (*EventSchemaMessage) Descriptor() ([]byte, []int) {
	return file_proto_event_schema_types_proto_rawDescGZIP(), []int{1}
}

func (x *EventSchemaMessage) GetWorkspaceID() string {
	if x != nil {
		return x.WorkspaceID
	}
	return ""
}

func (x *EventSchemaMessage) GetKey() *EventSchemaKey {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *EventSchemaMessage) GetSchema() map[string]string {
	if x != nil {
		return x.Schema
	}
	return nil
}

func (x *EventSchemaMessage) GetObservedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.ObservedAt
	}
	return nil
}

func (x *EventSchemaMessage) GetSample() []byte {
	if x != nil {
		return x.Sample
	}
	return nil
}

func (x *EventSchemaMessage) GetCorrelationID() []byte {
	if x != nil {
		return x.CorrelationID
	}
	return nil
}

func (x *EventSchemaMessage) GetHash() string {
	if x != nil {
		return x.Hash
	}
	return ""
}

func (x *EventSchemaMessage) GetBatchCount() int64 {
	if x != nil {
		return x.BatchCount
	}
	return 0
}

var File_proto_event_schema_types_proto protoreflect.FileDescriptor

var file_proto_event_schema_types_proto_rawDesc = []byte{
	0x0a, 0x1e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x2d, 0x73, 0x63,
	0x68, 0x65, 0x6d, 0x61, 0x2f, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x05, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61,
	0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x74, 0x0a, 0x0e, 0x45, 0x76, 0x65, 0x6e,
	0x74, 0x53, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x4b, 0x65, 0x79, 0x12, 0x1a, 0x0a, 0x08, 0x77, 0x72,
	0x69, 0x74, 0x65, 0x4b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x77, 0x72,
	0x69, 0x74, 0x65, 0x4b, 0x65, 0x79, 0x12, 0x1c, 0x0a, 0x09, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x54,
	0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x65, 0x76, 0x65, 0x6e, 0x74,
	0x54, 0x79, 0x70, 0x65, 0x12, 0x28, 0x0a, 0x0f, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x65,
	0x6e, 0x74, 0x69, 0x66, 0x69, 0x65, 0x72, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0f, 0x65,
	0x76, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x66, 0x69, 0x65, 0x72, 0x22, 0x87,
	0x03, 0x0a, 0x12, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x53, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x4d, 0x65,
	0x73, 0x73, 0x61, 0x67, 0x65, 0x12, 0x20, 0x0a, 0x0b, 0x77, 0x6f, 0x72, 0x6b, 0x73, 0x70, 0x61,
	0x63, 0x65, 0x49, 0x44, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x77, 0x6f, 0x72, 0x6b,
	0x73, 0x70, 0x61, 0x63, 0x65, 0x49, 0x44, 0x12, 0x27, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x15, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x45, 0x76, 0x65,
	0x6e, 0x74, 0x53, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x4b, 0x65, 0x79, 0x52, 0x03, 0x6b, 0x65, 0x79,
	0x12, 0x3d, 0x0a, 0x06, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x25, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x53, 0x63,
	0x68, 0x65, 0x6d, 0x61, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x53, 0x63, 0x68, 0x65,
	0x6d, 0x61, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x06, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x12,
	0x3a, 0x0a, 0x0a, 0x6f, 0x62, 0x73, 0x65, 0x72, 0x76, 0x65, 0x64, 0x41, 0x74, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52,
	0x0a, 0x6f, 0x62, 0x73, 0x65, 0x72, 0x76, 0x65, 0x64, 0x41, 0x74, 0x12, 0x16, 0x0a, 0x06, 0x73,
	0x61, 0x6d, 0x70, 0x6c, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x06, 0x73, 0x61, 0x6d,
	0x70, 0x6c, 0x65, 0x12, 0x24, 0x0a, 0x0d, 0x63, 0x6f, 0x72, 0x72, 0x65, 0x6c, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x49, 0x44, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x0d, 0x63, 0x6f, 0x72, 0x72,
	0x65, 0x6c, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x44, 0x12, 0x12, 0x0a, 0x04, 0x68, 0x61, 0x73,
	0x68, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x68, 0x61, 0x73, 0x68, 0x12, 0x1e, 0x0a,
	0x0a, 0x62, 0x61, 0x74, 0x63, 0x68, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x08, 0x20, 0x01, 0x28,
	0x03, 0x52, 0x0a, 0x62, 0x61, 0x74, 0x63, 0x68, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x1a, 0x39, 0x0a,
	0x0b, 0x53, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03,
	0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14,
	0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76,
	0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x42, 0x09, 0x5a, 0x07, 0x2e, 0x3b, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_proto_event_schema_types_proto_rawDescOnce sync.Once
	file_proto_event_schema_types_proto_rawDescData = file_proto_event_schema_types_proto_rawDesc
)

func file_proto_event_schema_types_proto_rawDescGZIP() []byte {
	file_proto_event_schema_types_proto_rawDescOnce.Do(func() {
		file_proto_event_schema_types_proto_rawDescData = protoimpl.X.CompressGZIP(file_proto_event_schema_types_proto_rawDescData)
	})
	return file_proto_event_schema_types_proto_rawDescData
}

var file_proto_event_schema_types_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_proto_event_schema_types_proto_goTypes = []interface{}{
	(*EventSchemaKey)(nil),        // 0: proto.EventSchemaKey
	(*EventSchemaMessage)(nil),    // 1: proto.EventSchemaMessage
	nil,                           // 2: proto.EventSchemaMessage.SchemaEntry
	(*timestamppb.Timestamp)(nil), // 3: google.protobuf.Timestamp
}
var file_proto_event_schema_types_proto_depIdxs = []int32{
	0, // 0: proto.EventSchemaMessage.key:type_name -> proto.EventSchemaKey
	2, // 1: proto.EventSchemaMessage.schema:type_name -> proto.EventSchemaMessage.SchemaEntry
	3, // 2: proto.EventSchemaMessage.observedAt:type_name -> google.protobuf.Timestamp
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_proto_event_schema_types_proto_init() }
func file_proto_event_schema_types_proto_init() {
	if File_proto_event_schema_types_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_proto_event_schema_types_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*EventSchemaKey); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_event_schema_types_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*EventSchemaMessage); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_proto_event_schema_types_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_proto_event_schema_types_proto_goTypes,
		DependencyIndexes: file_proto_event_schema_types_proto_depIdxs,
		MessageInfos:      file_proto_event_schema_types_proto_msgTypes,
	}.Build()
	File_proto_event_schema_types_proto = out.File
	file_proto_event_schema_types_proto_rawDesc = nil
	file_proto_event_schema_types_proto_goTypes = nil
	file_proto_event_schema_types_proto_depIdxs = nil
}

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.25.0
// 	protoc        v3.14.0
// source: proto/common/dpauth.proto

package proto

import (
	proto "github.com/golang/protobuf/proto"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

type GetWorkspaceTokenRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *GetWorkspaceTokenRequest) Reset() {
	*x = GetWorkspaceTokenRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_common_dpauth_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetWorkspaceTokenRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetWorkspaceTokenRequest) ProtoMessage() {}

func (x *GetWorkspaceTokenRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_common_dpauth_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetWorkspaceTokenRequest.ProtoReflect.Descriptor instead.
func (*GetWorkspaceTokenRequest) Descriptor() ([]byte, []int) {
	return file_proto_common_dpauth_proto_rawDescGZIP(), []int{0}
}

type GetWorkspaceTokenResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	WorkspaceToken string `protobuf:"bytes,1,opt,name=workspaceToken,proto3" json:"workspaceToken,omitempty"`
	Service        string `protobuf:"bytes,2,opt,name=service,proto3" json:"service,omitempty"`
	InstanceID     string `protobuf:"bytes,3,opt,name=instanceID,proto3" json:"instanceID,omitempty"`
}

func (x *GetWorkspaceTokenResponse) Reset() {
	*x = GetWorkspaceTokenResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_common_dpauth_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetWorkspaceTokenResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetWorkspaceTokenResponse) ProtoMessage() {}

func (x *GetWorkspaceTokenResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_common_dpauth_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetWorkspaceTokenResponse.ProtoReflect.Descriptor instead.
func (*GetWorkspaceTokenResponse) Descriptor() ([]byte, []int) {
	return file_proto_common_dpauth_proto_rawDescGZIP(), []int{1}
}

func (x *GetWorkspaceTokenResponse) GetWorkspaceToken() string {
	if x != nil {
		return x.WorkspaceToken
	}
	return ""
}

func (x *GetWorkspaceTokenResponse) GetService() string {
	if x != nil {
		return x.Service
	}
	return ""
}

func (x *GetWorkspaceTokenResponse) GetInstanceID() string {
	if x != nil {
		return x.InstanceID
	}
	return ""
}

var File_proto_common_dpauth_proto protoreflect.FileDescriptor

var file_proto_common_dpauth_proto_rawDesc = []byte{
	0x0a, 0x19, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2f, 0x64,
	0x70, 0x61, 0x75, 0x74, 0x68, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x05, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x22, 0x1a, 0x0a, 0x18, 0x47, 0x65, 0x74, 0x57, 0x6f, 0x72, 0x6b, 0x73, 0x70, 0x61,
	0x63, 0x65, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x22, 0x7d,
	0x0a, 0x19, 0x47, 0x65, 0x74, 0x57, 0x6f, 0x72, 0x6b, 0x73, 0x70, 0x61, 0x63, 0x65, 0x54, 0x6f,
	0x6b, 0x65, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x26, 0x0a, 0x0e, 0x77,
	0x6f, 0x72, 0x6b, 0x73, 0x70, 0x61, 0x63, 0x65, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x0e, 0x77, 0x6f, 0x72, 0x6b, 0x73, 0x70, 0x61, 0x63, 0x65, 0x54, 0x6f,
	0x6b, 0x65, 0x6e, 0x12, 0x18, 0x0a, 0x07, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x1e, 0x0a,
	0x0a, 0x69, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x49, 0x44, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x0a, 0x69, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x49, 0x44, 0x32, 0x67, 0x0a,
	0x0d, 0x44, 0x50, 0x41, 0x75, 0x74, 0x68, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x56,
	0x0a, 0x11, 0x47, 0x65, 0x74, 0x57, 0x6f, 0x72, 0x6b, 0x73, 0x70, 0x61, 0x63, 0x65, 0x54, 0x6f,
	0x6b, 0x65, 0x6e, 0x12, 0x1f, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x47, 0x65, 0x74, 0x57,
	0x6f, 0x72, 0x6b, 0x73, 0x70, 0x61, 0x63, 0x65, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x1a, 0x20, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x47, 0x65, 0x74,
	0x57, 0x6f, 0x72, 0x6b, 0x73, 0x70, 0x61, 0x63, 0x65, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x42, 0x09, 0x5a, 0x07, 0x2e, 0x3b, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_proto_common_dpauth_proto_rawDescOnce sync.Once
	file_proto_common_dpauth_proto_rawDescData = file_proto_common_dpauth_proto_rawDesc
)

func file_proto_common_dpauth_proto_rawDescGZIP() []byte {
	file_proto_common_dpauth_proto_rawDescOnce.Do(func() {
		file_proto_common_dpauth_proto_rawDescData = protoimpl.X.CompressGZIP(file_proto_common_dpauth_proto_rawDescData)
	})
	return file_proto_common_dpauth_proto_rawDescData
}

var file_proto_common_dpauth_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_proto_common_dpauth_proto_goTypes = []interface{}{
	(*GetWorkspaceTokenRequest)(nil),  // 0: proto.GetWorkspaceTokenRequest
	(*GetWorkspaceTokenResponse)(nil), // 1: proto.GetWorkspaceTokenResponse
}
var file_proto_common_dpauth_proto_depIdxs = []int32{
	0, // 0: proto.DPAuthService.GetWorkspaceToken:input_type -> proto.GetWorkspaceTokenRequest
	1, // 1: proto.DPAuthService.GetWorkspaceToken:output_type -> proto.GetWorkspaceTokenResponse
	1, // [1:2] is the sub-list for method output_type
	0, // [0:1] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_proto_common_dpauth_proto_init() }
func file_proto_common_dpauth_proto_init() {
	if File_proto_common_dpauth_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_proto_common_dpauth_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetWorkspaceTokenRequest); i {
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
		file_proto_common_dpauth_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetWorkspaceTokenResponse); i {
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
			RawDescriptor: file_proto_common_dpauth_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_proto_common_dpauth_proto_goTypes,
		DependencyIndexes: file_proto_common_dpauth_proto_depIdxs,
		MessageInfos:      file_proto_common_dpauth_proto_msgTypes,
	}.Build()
	File_proto_common_dpauth_proto = out.File
	file_proto_common_dpauth_proto_rawDesc = nil
	file_proto_common_dpauth_proto_goTypes = nil
	file_proto_common_dpauth_proto_depIdxs = nil
}

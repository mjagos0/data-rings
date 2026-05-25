package dhtpb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	_	= protoimpl.EnforceVersion(20 - protoimpl.MinVersion)

	_	= protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type SignedRecord struct {
	state		protoimpl.MessageState	`protogen:"open.v1"`
	Data		[]byte			`protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
	Pubkey		[]byte			`protobuf:"bytes,2,opt,name=pubkey,proto3" json:"pubkey,omitempty"`
	Signature	[]byte			`protobuf:"bytes,3,opt,name=signature,proto3" json:"signature,omitempty"`
	unknownFields	protoimpl.UnknownFields
	sizeCache	protoimpl.SizeCache
}

func (x *SignedRecord) Reset() {
	*x = SignedRecord{}
	mi := &file_dht_dhtpb_records_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SignedRecord) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SignedRecord) ProtoMessage()	{}

func (x *SignedRecord) ProtoReflect() protoreflect.Message {
	mi := &file_dht_dhtpb_records_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*SignedRecord) Descriptor() ([]byte, []int) {
	return file_dht_dhtpb_records_proto_rawDescGZIP(), []int{0}
}

func (x *SignedRecord) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *SignedRecord) GetPubkey() []byte {
	if x != nil {
		return x.Pubkey
	}
	return nil
}

func (x *SignedRecord) GetSignature() []byte {
	if x != nil {
		return x.Signature
	}
	return nil
}

type ProviderRecord struct {
	state		protoimpl.MessageState	`protogen:"open.v1"`
	ContentHash	[]byte			`protobuf:"bytes,1,opt,name=content_hash,json=contentHash,proto3" json:"content_hash,omitempty"`
	Provider	[]byte			`protobuf:"bytes,2,opt,name=provider,proto3" json:"provider,omitempty"`
	unknownFields	protoimpl.UnknownFields
	sizeCache	protoimpl.SizeCache
}

func (x *ProviderRecord) Reset() {
	*x = ProviderRecord{}
	mi := &file_dht_dhtpb_records_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ProviderRecord) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProviderRecord) ProtoMessage()	{}

func (x *ProviderRecord) ProtoReflect() protoreflect.Message {
	mi := &file_dht_dhtpb_records_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*ProviderRecord) Descriptor() ([]byte, []int) {
	return file_dht_dhtpb_records_proto_rawDescGZIP(), []int{1}
}

func (x *ProviderRecord) GetContentHash() []byte {
	if x != nil {
		return x.ContentHash
	}
	return nil
}

func (x *ProviderRecord) GetProvider() []byte {
	if x != nil {
		return x.Provider
	}
	return nil
}

type ProviderRecordList struct {
	state		protoimpl.MessageState	`protogen:"open.v1"`
	Providers	[]*ProviderRecord	`protobuf:"bytes,1,rep,name=providers,proto3" json:"providers,omitempty"`

	Timestamps	[]int64	`protobuf:"varint,2,rep,packed,name=timestamps,proto3" json:"timestamps,omitempty"`
	unknownFields	protoimpl.UnknownFields
	sizeCache	protoimpl.SizeCache
}

func (x *ProviderRecordList) Reset() {
	*x = ProviderRecordList{}
	mi := &file_dht_dhtpb_records_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ProviderRecordList) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProviderRecordList) ProtoMessage()	{}

func (x *ProviderRecordList) ProtoReflect() protoreflect.Message {
	mi := &file_dht_dhtpb_records_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*ProviderRecordList) Descriptor() ([]byte, []int) {
	return file_dht_dhtpb_records_proto_rawDescGZIP(), []int{2}
}

func (x *ProviderRecordList) GetProviders() []*ProviderRecord {
	if x != nil {
		return x.Providers
	}
	return nil
}

func (x *ProviderRecordList) GetTimestamps() []int64 {
	if x != nil {
		return x.Timestamps
	}
	return nil
}

var File_dht_dhtpb_records_proto protoreflect.FileDescriptor

const file_dht_dhtpb_records_proto_rawDesc = "" +
	"\n" +
	"\x17dht/dhtpb/records.proto\x12\x05dhtpb\"X\n" +
	"\fSignedRecord\x12\x12\n" +
	"\x04data\x18\x01 \x01(\fR\x04data\x12\x16\n" +
	"\x06pubkey\x18\x02 \x01(\fR\x06pubkey\x12\x1c\n" +
	"\tsignature\x18\x03 \x01(\fR\tsignature\"O\n" +
	"\x0eProviderRecord\x12!\n" +
	"\fcontent_hash\x18\x01 \x01(\fR\vcontentHash\x12\x1a\n" +
	"\bprovider\x18\x02 \x01(\fR\bprovider\"i\n" +
	"\x12ProviderRecordList\x123\n" +
	"\tproviders\x18\x01 \x03(\v2\x15.dhtpb.ProviderRecordR\tproviders\x12\x1e\n" +
	"\n" +
	"timestamps\x18\x02 \x03(\x03R\n" +
	"timestampsB(Z&github.com/mjagos0/datarings/dht/dhtpbb\x06proto3"

var (
	file_dht_dhtpb_records_proto_rawDescOnce	sync.Once
	file_dht_dhtpb_records_proto_rawDescData	[]byte
)

func file_dht_dhtpb_records_proto_rawDescGZIP() []byte {
	file_dht_dhtpb_records_proto_rawDescOnce.Do(func() {
		file_dht_dhtpb_records_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_dht_dhtpb_records_proto_rawDesc), len(file_dht_dhtpb_records_proto_rawDesc)))
	})
	return file_dht_dhtpb_records_proto_rawDescData
}

var file_dht_dhtpb_records_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_dht_dhtpb_records_proto_goTypes = []any{
	(*SignedRecord)(nil),
	(*ProviderRecord)(nil),
	(*ProviderRecordList)(nil),
}
var file_dht_dhtpb_records_proto_depIdxs = []int32{
	1,
	1,
	1,
	1,
	1,
	0,
}

func init()	{ file_dht_dhtpb_records_proto_init() }
func file_dht_dhtpb_records_proto_init() {
	if File_dht_dhtpb_records_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath:	reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor:	unsafe.Slice(unsafe.StringData(file_dht_dhtpb_records_proto_rawDesc), len(file_dht_dhtpb_records_proto_rawDesc)),
			NumEnums:	0,
			NumMessages:	3,
			NumExtensions:	0,
			NumServices:	0,
		},
		GoTypes:		file_dht_dhtpb_records_proto_goTypes,
		DependencyIndexes:	file_dht_dhtpb_records_proto_depIdxs,
		MessageInfos:		file_dht_dhtpb_records_proto_msgTypes,
	}.Build()
	File_dht_dhtpb_records_proto = out.File
	file_dht_dhtpb_records_proto_goTypes = nil
	file_dht_dhtpb_records_proto_depIdxs = nil
}

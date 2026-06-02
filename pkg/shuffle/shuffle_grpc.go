package shuffle

import (
	"context"
	"google.golang.org/grpc"
)

type PushRequest struct {
	JobId        string `protobuf:"bytes,1,opt,name=job_id,json=jobId,proto3"`
	TaskIndex    int32  `protobuf:"varint,2,opt,name=task_index,json=taskIndex,proto3"`
	ReducerIndex int32  `protobuf:"varint,3,opt,name=reducer_index,json=reducerIndex,proto3"`
	Data         []byte `protobuf:"bytes,4,opt,name=data,proto3"`
}

func (*PushRequest) ProtoMessage()  {}
func (x *PushRequest) Reset()         { *x = PushRequest{} }
func (x *PushRequest) String() string { return "PushRequest" }

type PushResponse struct {
	Success bool `protobuf:"varint,1,opt,name=success,proto3"`
}

func (*PushResponse) ProtoMessage()  {}
func (x *PushResponse) Reset()         { *x = PushResponse{} }
func (x *PushResponse) String() string { return "PushResponse" }

type PullRequest struct {
	JobId        string `protobuf:"bytes,1,opt,name=job_id,json=jobId,proto3"`
	ReducerIndex int32  `protobuf:"varint,2,opt,name=reducer_index,json=reducerIndex,proto3"`
}

func (*PullRequest) ProtoMessage()  {}
func (x *PullRequest) Reset()         { *x = PullRequest{} }
func (x *PullRequest) String() string { return "PullRequest" }

type PullResponse struct {
	Data []byte `protobuf:"bytes,1,opt,name=data,proto3"`
}

func (*PullResponse) ProtoMessage()  {}
func (x *PullResponse) Reset()         { *x = PullResponse{} }
func (x *PullResponse) String() string { return "PullResponse" }

type CleanupRequest struct {
	JobId string `protobuf:"bytes,1,opt,name=job_id,json=jobId,proto3"`
}

func (*CleanupRequest) ProtoMessage()  {}
func (x *CleanupRequest) Reset()         { *x = CleanupRequest{} }
func (x *CleanupRequest) String() string { return "CleanupRequest" }

type CleanupResponse struct {
	Success bool `protobuf:"varint,1,opt,name=success,proto3"`
}

func (*CleanupResponse) ProtoMessage()  {}
func (x *CleanupResponse) Reset()         { *x = CleanupResponse{} }
func (x *CleanupResponse) String() string { return "CleanupResponse" }

type ShuffleService_PushServer interface {
	SendAndClose(*PushResponse) error
	Recv() (*PushRequest, error)
	grpc.ServerStream
}

type ShuffleService_PullServer interface {
	Send(*PullResponse) error
	grpc.ServerStream
}

type ShuffleServiceServer interface {
	Push(ShuffleService_PushServer) error
	Pull(*PullRequest, ShuffleService_PullServer) error
	Cleanup(context.Context, *CleanupRequest) (*CleanupResponse, error)
}

type UnimplementedShuffleServiceServer struct{}

func (UnimplementedShuffleServiceServer) Push(ShuffleService_PushServer) error {
	return nil
}
func (UnimplementedShuffleServiceServer) Pull(*PullRequest, ShuffleService_PullServer) error {
	return nil
}
func (UnimplementedShuffleServiceServer) Cleanup(context.Context, *CleanupRequest) (*CleanupResponse, error) {
	return nil, nil
}

var ShuffleService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "shuffle.ShuffleService",
	HandlerType: (*ShuffleServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Cleanup",
			Handler:    _ShuffleService_Cleanup_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Push",
			Handler:       _ShuffleService_Push_Handler,
			ServerStreams: false,
			ClientStreams: true,
		},
		{
			StreamName:    "Pull",
			Handler:       _ShuffleService_Pull_Handler,
			ServerStreams: true,
			ClientStreams: false,
		},
	},
	Metadata: "shuffle.proto",
}

func RegisterShuffleServiceServer(s grpc.ServiceRegistrar, srv ShuffleServiceServer) {
	s.RegisterService(&ShuffleService_ServiceDesc, srv)
}

func _ShuffleService_Cleanup_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CleanupRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ShuffleServiceServer).Cleanup(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/shuffle.ShuffleService/Cleanup",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ShuffleServiceServer).Cleanup(ctx, req.(*CleanupRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ShuffleService_Push_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(ShuffleServiceServer).Push(&shuffleServicePushServer{stream})
}

type shuffleServicePushServer struct {
	grpc.ServerStream
}

func (x *shuffleServicePushServer) SendAndClose(m *PushResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *shuffleServicePushServer) Recv() (*PushRequest, error) {
	m := new(PushRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _ShuffleService_Pull_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(PullRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(ShuffleServiceServer).Pull(m, &shuffleServicePullServer{stream})
}

type shuffleServicePullServer struct {
	grpc.ServerStream
}

func (x *shuffleServicePullServer) Send(m *PullResponse) error {
	return x.ServerStream.SendMsg(m)
}

type ShuffleServiceClient interface {
	Push(ctx context.Context, opts ...grpc.CallOption) (ShuffleService_PushClient, error)
	Pull(ctx context.Context, in *PullRequest, opts ...grpc.CallOption) (ShuffleService_PullClient, error)
	Cleanup(ctx context.Context, in *CleanupRequest, opts ...grpc.CallOption) (*CleanupResponse, error)
}

type ShuffleService_PushClient interface {
	Send(*PushRequest) error
	CloseAndRecv() (*PushResponse, error)
	grpc.ClientStream
}

type ShuffleService_PullClient interface {
	Recv() (*PullResponse, error)
	grpc.ClientStream
}

type shuffleServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewShuffleServiceClient(cc grpc.ClientConnInterface) ShuffleServiceClient {
	return &shuffleServiceClient{cc}
}

func (c *shuffleServiceClient) Push(ctx context.Context, opts ...grpc.CallOption) (ShuffleService_PushClient, error) {
	stream, err := c.cc.NewStream(ctx, &grpc.StreamDesc{
		StreamName:    "Push",
		ServerStreams: false,
		ClientStreams: true,
	}, "/shuffle.ShuffleService/Push", opts...)
	if err != nil {
		return nil, err
	}
	x := &shuffleServicePushClient{stream}
	return x, nil
}

func (c *shuffleServiceClient) Pull(ctx context.Context, in *PullRequest, opts ...grpc.CallOption) (ShuffleService_PullClient, error) {
	stream, err := c.cc.NewStream(ctx, &grpc.StreamDesc{
		StreamName:    "Pull",
		ServerStreams: true,
		ClientStreams: false,
	}, "/shuffle.ShuffleService/Pull", opts...)
	if err != nil {
		return nil, err
	}
	x := &shuffleServicePullClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

func (c *shuffleServiceClient) Cleanup(ctx context.Context, in *CleanupRequest, opts ...grpc.CallOption) (*CleanupResponse, error) {
	out := new(CleanupResponse)
	err := c.cc.Invoke(ctx, "/shuffle.ShuffleService/Cleanup", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

type shuffleServicePushClient struct {
	grpc.ClientStream
}

func (x *shuffleServicePushClient) Send(m *PushRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *shuffleServicePushClient) CloseAndRecv() (*PushResponse, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(PushResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

type shuffleServicePullClient struct {
	grpc.ClientStream
}

func (x *shuffleServicePullClient) Recv() (*PullResponse, error) {
	m := new(PullResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

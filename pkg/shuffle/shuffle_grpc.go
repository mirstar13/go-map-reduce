package shuffle

import (
	"context"
	"google.golang.org/grpc"
)

type PushRequest struct {
	JobId        string
	TaskIndex    int32
	ReducerIndex int32
	Data         []byte
}

type PushResponse struct {
	Success bool
}

type PullRequest struct {
	JobId        string
	ReducerIndex int32
}

type PullResponse struct {
	Data []byte
}

type CleanupRequest struct {
	JobId string
}

type CleanupResponse struct {
	Success bool
}

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

func RegisterShuffleServiceServer(s grpc.ServiceRegistrar, srv ShuffleServiceServer) {
	// Mock registration to allow compilation without protoc-generated code
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

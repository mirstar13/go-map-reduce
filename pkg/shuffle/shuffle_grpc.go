package shuffle

import (
	"context"
	"google.golang.org/grpc"
)

type PushRequest struct {
	JobId        string
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

package main

import (
	"context"
	"io"
	"sync"
	"github.com/mirstar13/go-map-reduce/pkg/shuffle"
)

type Server struct {
	mu    sync.RWMutex
	data  map[string]map[int32][][]byte
}

func NewServer() *Server {
	return &Server{data: make(map[string]map[int32][][]byte)}
}

func (s *Server) Push(stream shuffle.ShuffleService_PushServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&shuffle.PushResponse{Success: true})
		}
		if err != nil { return err }
		s.mu.Lock()
		if s.data[req.JobId] == nil { s.data[req.JobId] = make(map[int32][][]byte) }
		s.data[req.JobId][req.ReducerIndex] = append(s.data[req.JobId][req.ReducerIndex], req.Data)
		s.mu.Unlock()
	}
}

func (s *Server) Pull(req *shuffle.PullRequest, stream shuffle.ShuffleService_PullServer) error {
	s.mu.RLock()
	jobData, ok := s.data[req.JobId]
	if !ok { s.mu.RUnlock(); return nil }
	chunks := jobData[req.ReducerIndex]
	s.mu.RUnlock()
	for _, chunk := range chunks {
		if err := stream.Send(&shuffle.PullResponse{Data: chunk}); err != nil { return err }
	}
	return nil
}

func (s *Server) Cleanup(ctx context.Context, req *shuffle.CleanupRequest) (*shuffle.CleanupResponse, error) {
	s.mu.Lock()
	delete(s.data, req.JobId)
	s.mu.Unlock()
	return &shuffle.CleanupResponse{Success: true}, nil
}

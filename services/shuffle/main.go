package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"
	"github.com/mirstar13/go-map-reduce/pkg/shuffle"
)

func main() {
	port := flag.Int("port", 50051, "The server port")
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		fmt.Printf("failed to listen: %v\n", err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	shuffle.RegisterShuffleServiceServer(s, NewServer())

	fmt.Printf("Shuffle service listening on %v\n", lis.Addr())
	if err := s.Serve(lis); err != nil {
		fmt.Printf("failed to serve: %v\n", err)
		os.Exit(1)
	}
}

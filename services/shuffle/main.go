package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus/promhttp"
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

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		fmt.Println("Metrics server listening on :9091")
		if err := http.ListenAndServe(":9091", nil); err != nil {
			fmt.Printf("metrics server failed: %v\n", err)
		}
	}()

	fmt.Printf("Shuffle service listening on %v\n", lis.Addr())
	if err := s.Serve(lis); err != nil {
		fmt.Printf("failed to serve: %v\n", err)
		os.Exit(1)
	}
}

package plugin

import (
	"context"
	"net/rpc"

	"github.com/hashicorp/go-plugin"
)

// Handshake is used to verify that the plugin and host are compatible.
// This should be kept in sync across all plugin versions.
var Handshake = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "MAPREDUCE_PLUGIN",
	MagicCookieValue: "mapreduce-v1",
}

// PluginMap is the map of plugin types available.
var PluginMap = map[string]plugin.Plugin{
	"mapper":  &MapperPlugin{},
	"reducer": &ReducerPlugin{},
}

// MapperPlugin is the plugin.Plugin implementation for Mapper.
type MapperPlugin struct {
	Impl Mapper
}

func (p *MapperPlugin) Server(*plugin.MuxBroker) (interface{}, error) {
	return &MapperRPCServer{Impl: p.Impl}, nil
}

func (p *MapperPlugin) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &MapperRPCClient{client: c}, nil
}

// ReducerPlugin is the plugin.Plugin implementation for Reducer.
type ReducerPlugin struct {
	Impl Reducer
}

func (p *ReducerPlugin) Server(*plugin.MuxBroker) (interface{}, error) {
	return &ReducerRPCServer{Impl: p.Impl}, nil
}

func (p *ReducerPlugin) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &ReducerRPCClient{client: c}, nil
}

// MapperRPCClient is an RPC client implementation of Mapper.
type MapperRPCClient struct {
	client *rpc.Client
}

// MapArgs holds the arguments for Map RPC calls.
type MapArgs struct {
	Key   string
	Value string
}

// MapReply holds the response from Map RPC calls.
type MapReply struct {
	Records []Record
	Error   string
}

func (m *MapperRPCClient) Map(key, value string) ([]Record, error) {
	var reply MapReply
	err := m.client.Call("Plugin.Map", &MapArgs{Key: key, Value: value}, &reply)
	if err != nil {
		return nil, err
	}
	if reply.Error != "" {
		return nil, &PluginError{Message: reply.Error}
	}
	return reply.Records, nil
}

// MapperRPCServer is an RPC server implementation that wraps a Mapper.
type MapperRPCServer struct {
	Impl Mapper
}

func (s *MapperRPCServer) Map(args *MapArgs, reply *MapReply) error {
	records, err := s.Impl.Map(args.Key, args.Value)
	if err != nil {
		reply.Error = err.Error()
		return nil
	}
	reply.Records = records
	return nil
}

// ReducerRPCClient is an RPC client implementation of Reducer.
type ReducerRPCClient struct {
	client *rpc.Client
}

// ReduceArgs holds the arguments for Reduce RPC calls.
type ReduceArgs struct {
	Key    string
	Values []string
}

// ReduceReply holds the response from Reduce RPC calls.
type ReduceReply struct {
	Records []Record
	Error   string
}

func (r *ReducerRPCClient) Reduce(key string, values []string) ([]Record, error) {
	var reply ReduceReply
	err := r.client.Call("Plugin.Reduce", &ReduceArgs{Key: key, Values: values}, &reply)
	if err != nil {
		return nil, err
	}
	if reply.Error != "" {
		return nil, &PluginError{Message: reply.Error}
	}
	return reply.Records, nil
}

// ReducerRPCServer is an RPC server implementation that wraps a Reducer.
type ReducerRPCServer struct {
	Impl Reducer
}

func (s *ReducerRPCServer) Reduce(args *ReduceArgs, reply *ReduceReply) error {
	records, err := s.Impl.Reduce(args.Key, args.Values)
	if err != nil {
		reply.Error = err.Error()
		return nil
	}
	reply.Records = records
	return nil
}

// PluginError represents an error returned from a plugin.
type PluginError struct {
	Message string
}

func (e *PluginError) Error() string {
	return e.Message
}

// LoadMapper loads a mapper plugin from the given path.
func LoadMapper(ctx context.Context, path string) (Mapper, func(), error) {
	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: Handshake,
		Plugins:         PluginMap,
		Cmd:             pluginCmd(path),
		AllowedProtocols: []plugin.Protocol{
			plugin.ProtocolNetRPC,
		},
	})

	rpcClient, err := client.Client()
	if err != nil {
		client.Kill()
		return nil, nil, err
	}

	raw, err := rpcClient.Dispense("mapper")
	if err != nil {
		client.Kill()
		return nil, nil, err
	}

	mapper, ok := raw.(Mapper)
	if !ok {
		client.Kill()
		return nil, nil, &PluginError{Message: "plugin does not implement Mapper interface"}
	}

	cleanup := func() {
		client.Kill()
	}

	return mapper, cleanup, nil
}

// LoadReducer loads a reducer plugin from the given path.
func LoadReducer(ctx context.Context, path string) (Reducer, func(), error) {
	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: Handshake,
		Plugins:         PluginMap,
		Cmd:             pluginCmd(path),
		AllowedProtocols: []plugin.Protocol{
			plugin.ProtocolNetRPC,
		},
	})

	rpcClient, err := client.Client()
	if err != nil {
		client.Kill()
		return nil, nil, err
	}

	raw, err := rpcClient.Dispense("reducer")
	if err != nil {
		client.Kill()
		return nil, nil, err
	}

	reducer, ok := raw.(Reducer)
	if !ok {
		client.Kill()
		return nil, nil, &PluginError{Message: "plugin does not implement Reducer interface"}
	}

	cleanup := func() {
		client.Kill()
	}

	return reducer, cleanup, nil
}

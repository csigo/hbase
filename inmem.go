package hbase

import (
	"fmt"
	"log"
	"net"

	"git.apache.org/thrift.git/lib/go/thrift"
)

// TestServer defines a server structure for testing purpose
type TestServer struct {
	Port int
	stop chan struct{}
}

// Stop stop TestServer
func (t *TestServer) Stop() {
	close(t.stop)
}

// NewHbaseServer starts an self-implementation hbase
func NewHbaseServer(hb Hbase) (*TestServer, error) {

	port, _ := GetPort()
	addr := fmt.Sprintf(":%d", port)

	// fork a goroutine to serve requests
	var transportFactory thrift.TTransportFactory
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	transportFactory = thrift.NewTBufferedTransportFactory(8192)
	transportFactory = thrift.NewTFramedTransportFactory(transportFactory)
	transport, err := thrift.NewTServerSocket(addr)
	if err != nil {
		log.Fatal(err)
	}
	srv := thrift.NewTSimpleServer4(
		NewHbaseProcessor(hb),
		transport,
		transportFactory,
		protocolFactory,
	)
	if err := srv.Listen(); err != nil {
		log.Fatal(err)
	}
	go srv.AcceptLoop()

	// TODO: stop server when stop chan is closed
	return &TestServer{
		Port: port,
		stop: make(chan struct{}),
	}, nil
}

// GetPort gets a free port
func GetPort() (int, error) {
	l, err := net.Listen("tcp4", ":0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

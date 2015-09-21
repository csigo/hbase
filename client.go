package hbase

import (
	"io"
	"sync"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
)

var (
	defaultTimeout    = 30 * time.Second
	defaultBufferSize = 8192
)

// clientCloser implements the interface of Hbase
type clientCloser struct {
	*HbaseClient

	// mu is used to lock the underlying conn to ensure thread safe
	mu   *sync.Mutex
	conn *thrift.TSocket
}

func (c *clientCloser) Close() error {
	return c.conn.Close()
}

// ThriftClientFactory is an thrift Client factory which creates a connection
// that uses a thrift codec.
func ThriftClientFactory(addr string) func() (io.Closer, error) {
	return func() (io.Closer, error) {
		var transport thrift.TTransport
		socketTransport, err := thrift.NewTSocketTimeout(addr, defaultTimeout)
		if err != nil {
			return nil, err
		}
		transport = thrift.NewTFramedTransport(
			thrift.NewTBufferedTransport(socketTransport, defaultBufferSize))
		protocol := thrift.NewTBinaryProtocolFactoryDefault()
		if err := transport.Open(); err != nil {
			return nil, err
		}

		client := NewHbaseClientFactory(transport, protocol)
		return &clientCloser{
			mu:          &sync.Mutex{},
			conn:        socketTransport,
			HbaseClient: client,
		}, nil
	}
}

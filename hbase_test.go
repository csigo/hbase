package hbase

import (
	"fmt"
	"testing"
)

func TestHbase(t *testing.T) {
	mockServer := &MockHbase{}
	mockServer.On("IsTableEnabled", Bytes("existTable")).Return(true, nil)

	srv, err := NewHbaseServer(mockServer)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	connFactory := ThriftClientFactory(
		fmt.Sprintf("127.0.0.1:%d", srv.Port))
	rawConn, err := connFactory()
	if err != nil {
		t.Fatal(err)
	}
	defer rawConn.Close()

	hConn := NewConn(rawConn)
	shouldBeTrue, err := hConn.IsTableEnabled(Bytes("existTable"))
	if err != nil {
		t.Fatal(err)
	}
	if !shouldBeTrue {
		t.Fatalf("should be true, but got false")
	}
}

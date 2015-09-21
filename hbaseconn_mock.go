package hbase

// MockHBaseConn is a mock implementation of HBaseConn
type MockHBaseConn struct {
	MockHbase
}

// Close is a mock method
func (m *MockHBaseConn) Close() {
	m.MockHbase.Called()
}

// Recycle is a mock method
func (m *MockHBaseConn) Recycle() {
	m.MockHbase.Called()
}

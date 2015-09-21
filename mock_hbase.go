package hbase

import (
	"github.com/stretchr/testify/mock"
)

// MockHbase is a mock implementation of the Hbase interface that is used only
// in the package-local unit tests.
type MockHbase struct {
	mock.Mock
}

// NoErrorReturningFunc is a mock function
func (m *MockHbase) NoErrorReturningFunc() {}

// InvalidErrorReturningFunc is a mock function
func (m *MockHbase) InvalidErrorReturningFunc() (error, bool) {
	return nil, false
}

// AtomicIncrement is a mock function
func (m *MockHbase) AtomicIncrement(tableName Text, row Text, column Text, value int64) (int64, error) {
	args := m.Called(tableName, row, column, value)
	return args.Get(0).(int64), args.Error(1)
}

// Compact is a mock function
func (m *MockHbase) Compact(tableNameOrRegionName Bytes) error {
	args := m.Called(tableNameOrRegionName)
	return args.Error(0)
}

// CreateTable is a mock function
func (m *MockHbase) CreateTable(tableName Text, columnFamilies []*ColumnDescriptor) error {
	args := m.Called(tableName, columnFamilies)
	return args.Error(0)
}

// DeleteAll is a mock function
func (m *MockHbase) DeleteAll(tableName Text, row Text, column Text, attributes map[string]Text) error {
	args := m.Called(tableName, row, column, attributes)
	return args.Error(0)
}

// DeleteAllRow is a mock function
func (m *MockHbase) DeleteAllRow(tableName Text, row Text, attributes map[string]Text) error {
	args := m.Called(tableName, row, attributes)
	return args.Error(0)
}

// DeleteAllRowTs is a mock function
func (m *MockHbase) DeleteAllRowTs(tableName Text, row Text, timestamp int64, attributes map[string]Text) error {
	args := m.Called(tableName, row, timestamp, attributes)
	return args.Error(0)
}

// DeleteAllTs is a mock function
func (m *MockHbase) DeleteAllTs(tableName Text, row Text, column Text, timestamp int64, attributes map[string]Text) error {
	args := m.Called(tableName, row, column, timestamp, attributes)
	return args.Error(0)
}

// DeleteTable is a mock function
func (m *MockHbase) DeleteTable(tableName Text) error {
	args := m.Called(tableName)
	return args.Error(0)
}

// DisableTable is a mock function
func (m *MockHbase) DisableTable(tableName Bytes) error {
	args := m.Called(tableName)
	return args.Error(0)
}

// EnableTable is a mock function
func (m *MockHbase) EnableTable(tableName Bytes) error {
	args := m.Called(tableName)
	return args.Error(0)
}

// Get is a mock function
func (m *MockHbase) Get(tableName Text, row Text, column Text, attributes map[string]Text) ([]*TCell, error) {
	args := m.Called(tableName, row, column, attributes)
	return args.Get(0).([]*TCell), args.Error(1)
}

// GetColumnDescriptors is a mock function
func (m *MockHbase) GetColumnDescriptors(tableName Text) (map[string]*ColumnDescriptor, error) {
	args := m.Called(tableName)
	return args.Get(0).(map[string]*ColumnDescriptor), args.Error(1)
}

// GetRegionInfo is a mock function
func (m *MockHbase) GetRegionInfo(row Text) (*TRegionInfo, error) {
	args := m.Called(row)
	return args.Get(0).(*TRegionInfo), args.Error(1)
}

// GetRow is a mock function
func (m *MockHbase) GetRow(tableName Text, row Text, attributes map[string]Text) ([]*TRowResult_, error) {
	args := m.Called(tableName, row, attributes)
	return args.Get(0).([]*TRowResult_), args.Error(1)
}

// GetRowOrBefore is a mock function
func (m *MockHbase) GetRowOrBefore(tableName Text, row Text, family Text) ([]*TCell, error) {
	args := m.Called(tableName, row, family)
	return args.Get(0).([]*TCell), args.Error(1)
}

// GetRowTs is a mock function
func (m *MockHbase) GetRowTs(tableName Text, row Text, timestamp int64, attributes map[string]Text) ([]*TRowResult_, error) {
	args := m.Called(tableName, row, timestamp, attributes)
	return args.Get(0).([]*TRowResult_), args.Error(1)
}

// GetRowWithColumns is a mock function
func (m *MockHbase) GetRowWithColumns(tableName Text, row Text, columns [][]byte, attributes map[string]Text) ([]*TRowResult_, error) {
	args := m.Called(tableName, row, columns, attributes)
	return args.Get(0).([]*TRowResult_), args.Error(1)
}

// GetRowWithColumnsTs is a mock function
func (m *MockHbase) GetRowWithColumnsTs(tableName Text, row Text, columns [][]byte, timestamp int64, attributes map[string]Text) ([]*TRowResult_, error) {
	args := m.Called(tableName, row, columns, timestamp, attributes)
	return args.Get(0).([]*TRowResult_), args.Error(1)
}

// GetRows is a mock function
func (m *MockHbase) GetRows(tableName Text, rows [][]byte, attributes map[string]Text) ([]*TRowResult_, error) {
	args := m.Called(tableName, rows, attributes)
	return args.Get(0).([]*TRowResult_), args.Error(1)
}

// GetRowsTs is a mock function
func (m *MockHbase) GetRowsTs(tableName Text, rows [][]byte, timestamp int64, attributes map[string]Text) ([]*TRowResult_, error) {
	args := m.Called(tableName, rows, timestamp, attributes)
	return args.Get(0).([]*TRowResult_), args.Error(1)
}

// GetRowsWithColumns is a mock function
func (m *MockHbase) GetRowsWithColumns(tableName Text, rows [][]byte, columns [][]byte, attributes map[string]Text) ([]*TRowResult_, error) {
	args := m.Called(tableName, rows, columns, attributes)
	return args.Get(0).([]*TRowResult_), args.Error(1)
}

// GetRowsWithColumnsTs is a mock function
func (m *MockHbase) GetRowsWithColumnsTs(tableName Text, rows [][]byte, columns [][]byte, timestamp int64, attributes map[string]Text) ([]*TRowResult_, error) {
	args := m.Called(tableName, rows, columns, timestamp, attributes)
	return args.Get(0).([]*TRowResult_), args.Error(1)
}

// GetTableNames is a mock function
func (m *MockHbase) GetTableNames() ([][]byte, error) {
	args := m.Called()
	return args.Get(0).([][]byte), args.Error(1)
}

// GetTableRegions is a mock function
func (m *MockHbase) GetTableRegions(tableName Text) ([]*TRegionInfo, error) {
	args := m.Called(tableName)
	return args.Get(0).([]*TRegionInfo), args.Error(1)
}

// GetVer is a mock function
func (m *MockHbase) GetVer(tableName Text, row Text, column Text, numVersions int32, attributes map[string]Text) ([]*TCell, error) {
	args := m.Called(tableName, row, column, numVersions, attributes)
	return args.Get(0).([]*TCell), args.Error(1)
}

// GetVerTs is a mock function
func (m *MockHbase) GetVerTs(tableName Text, row Text, column Text, timestamp int64, numVersions int32, attributes map[string]Text) ([]*TCell, error) {
	args := m.Called(tableName, row, column, timestamp, numVersions, attributes)
	return args.Get(0).([]*TCell), args.Error(1)
}

// Increment is a mock function
func (m *MockHbase) Increment(increment *TIncrement) error {
	args := m.Called(increment)
	return args.Error(0)
}

// IncrementRows is a mock function
func (m *MockHbase) IncrementRows(increments []*TIncrement) error {
	args := m.Called(increments)
	return args.Error(0)
}

// IsTableEnabled is a mock function
func (m *MockHbase) IsTableEnabled(tableName Bytes) (bool, error) {
	args := m.Called(tableName)
	return args.Bool(0), args.Error(1)
}

// MajorCompact is a mock function
func (m *MockHbase) MajorCompact(tableNameOrRegionName Bytes) error {
	args := m.Called(tableNameOrRegionName)
	return args.Error(0)
}

// MutateRow is a mock function
func (m *MockHbase) MutateRow(tableName Text, row Text, mutations []*Mutation, attributes map[string]Text) error {
	args := m.Called(tableName, row, mutations, attributes)
	return args.Error(0)
}

// MutateRowTs is a mock function
func (m *MockHbase) MutateRowTs(tableName Text, row Text, mutations []*Mutation, timestamp int64, attributes map[string]Text) error {
	args := m.Called(tableName, row, mutations, timestamp, attributes)
	return args.Error(0)
}

// MutateRows is a mock function
func (m *MockHbase) MutateRows(tableName Text, rowBatches []*BatchMutation, attributes map[string]Text) error {
	args := m.Called(tableName, rowBatches, attributes)
	return args.Error(0)
}

// MutateRowsTs is a mock function
func (m *MockHbase) MutateRowsTs(tableName Text, rowBatches []*BatchMutation, timestamp int64, attributes map[string]Text) error {
	args := m.Called(tableName, rowBatches, timestamp, attributes)
	return args.Error(0)
}

// ScannerClose is a mock function
func (m *MockHbase) ScannerClose(id ScannerID) error {
	args := m.Called(id)
	return args.Error(0)
}

// ScannerGet is a mock function
func (m *MockHbase) ScannerGet(id ScannerID) ([]*TRowResult_, error) {
	args := m.Called(id)
	return args.Get(0).([]*TRowResult_), args.Error(1)
}

// ScannerGetList is a mock function
func (m *MockHbase) ScannerGetList(id ScannerID, nbRows int32) ([]*TRowResult_, error) {
	args := m.Called(id, nbRows)
	return args.Get(0).([]*TRowResult_), args.Error(1)
}

// ScannerOpen is a mock function
func (m *MockHbase) ScannerOpen(tableName Text, startRow Text, columns [][]byte, attributes map[string]Text) (ScannerID, error) {
	args := m.Called(tableName, startRow, columns, attributes)
	return args.Get(0).(ScannerID), args.Error(1)
}

// ScannerOpenTs is a mock function
func (m *MockHbase) ScannerOpenTs(tableName Text, startRow Text, columns [][]byte, timestamp int64, attributes map[string]Text) (ScannerID, error) {
	args := m.Called(tableName, startRow, columns, timestamp, attributes)
	return args.Get(0).(ScannerID), args.Error(1)
}

// ScannerOpenWithPrefix is a mock function
func (m *MockHbase) ScannerOpenWithPrefix(tableName Text, startAndPrefix Text, columns [][]byte, attributes map[string]Text) (ScannerID, error) {
	args := m.Called(tableName, startAndPrefix, columns, attributes)
	return args.Get(0).(ScannerID), args.Error(1)
}

// ScannerOpenWithScan is a mock function
func (m *MockHbase) ScannerOpenWithScan(tableName Text, scan *TScan, attributes map[string]Text) (ScannerID, error) {
	args := m.Called(tableName, scan, attributes)
	return args.Get(0).(ScannerID), args.Error(1)
}

// ScannerOpenWithStop is a mock function
func (m *MockHbase) ScannerOpenWithStop(tableName Text, startRow Text, stopRow Text, columns [][]byte, attributes map[string]Text) (ScannerID, error) {
	args := m.Called(tableName, startRow, stopRow, columns, attributes)
	return args.Get(0).(ScannerID), args.Error(1)
}

// ScannerOpenWithStopTs is a mock function
func (m *MockHbase) ScannerOpenWithStopTs(tableName Text, startRow Text, stopRow Text, columns [][]byte, timestamp int64, attributes map[string]Text) (ScannerID, error) {
	args := m.Called(tableName, startRow, stopRow, columns, timestamp, attributes)
	return args.Get(0).(ScannerID), args.Error(1)
}

// Appends values to one or more columns within a single row.
//
// @return values of columns after the append operation.
//
// Parameters:
//  - Append: The single append operation to apply
func (m *MockHbase) Append(append *TAppend) (r []*TCell, err error) {
	args := m.Called(append)
	return args.Get(0).([]*TCell), args.Error(1)
}

// Atomically checks if a row/family/qualifier value matches the expected
// value. If it does, it adds the corresponding mutation operation for put.
//
// @return true if the new put was executed, false otherwise
//
// Parameters:
//  - TableName: name of table
//  - Row: row key
//  - Column: column name
//  - Value: the expected value for the column parameter, if not
// provided the check is for the non-existence of the
// column in question
//  - Mput: mutation for the put
//  - Attributes: Mutation attributes
func (m *MockHbase) CheckAndPut(tableName Text, row Text, column Text, value Text, mput *Mutation, attributes map[string]Text) (r bool, err error) {
	args := m.Called(tableName, row, column, value, mput, attributes)
	return args.Bool(0), args.Error(1)
}

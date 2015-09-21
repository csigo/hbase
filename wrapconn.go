package hbase

import (
	"errors"
	"fmt"
	"io"
	"reflect"
)

// NewConn wraps a rawConn(i.e. io.Closer) into hbase conn
func NewConn(client io.Closer) *WrapConn {
	if _, ok := client.(*clientCloser); !ok {
		// unable to cast
		return nil
	}
	return &WrapConn{
		client: client.(*clientCloser),
	}
}

// WrapConn represents a connection to a HBase server.
// which implements HbaseConn and Hbase interface and also guarantee thread-safe property
type WrapConn struct {
	client *clientCloser
}

// invokeMethodViaReflection invokes the given method cmd on the obj with
// arguments args via reflection.  It is assumed that the last returned
// value of the invoked method is of error type and is returned as the
// first returned value of this function.
func invokeMethodViaReflection(obj interface{}, cmd string, args ...interface{}) (error, []reflect.Value) {

	objVal := reflect.ValueOf(obj)
	if !objVal.IsValid() {
		return fmt.Errorf("object value is invalid: %v", obj), nil
	}
	method := objVal.MethodByName(cmd)
	if !method.IsValid() {
		return fmt.Errorf("no method can be found with name %s", cmd), nil
	}

	numIn := len(args)
	if method.Type().IsVariadic() || method.Type().NumIn() != numIn {
		return fmt.Errorf("method with name %s has a mismatched number of arguments: [expected=%d, got=%d]",
			cmd, method.Type().NumIn(), numIn), nil
	}

	rargs := make([]reflect.Value, numIn)
	for i := 0; i < numIn; i++ {
		rargs[i] = reflect.ValueOf(args[i])
	}
	if method.Type().NumOut() < 1 {
		return errors.New("invoked method should at least return an error type"), nil
	}

	results := method.Call(rargs)
	numOut := len(results)
	if method.Type().NumOut() != numOut {
		return fmt.Errorf("method with name %s has a mismatched number of outputs: [expected=%d, got=%d]",
			cmd, method.Type().NumOut(), numOut), nil
	}

	if !method.Type().Out(numOut - 1).Implements(reflect.TypeOf(new(error)).Elem()) {
		return fmt.Errorf("last returned value does not implement error: %v", method.Type().Out(numOut-1)), nil
	}

	last := results[numOut-1]
	err, ok := last.Interface().(error)
	if err != nil && !ok {
		return fmt.Errorf("last returned value is not of error type: %v", last.Kind()), nil
	}

	return err, results[0 : len(results)-1]
}

// runCommand is a helper function that runs a given command via reflection.
// The results are returned as a slice of reflect.Value's.  It is the responsibility
// of the caller to determine and cast the resulting reflect.Value into the correct
// type for further use.
func (c *WrapConn) runCommand(cmd string, args ...interface{}) (err error, r []reflect.Value) {
	c.client.mu.Lock()
	err, r = invokeMethodViaReflection(c.client, cmd, args...)
	c.client.mu.Unlock()
	return
}

// AtomicIncrement wraps HBase AtomicIncrement method.
func (c *WrapConn) AtomicIncrement(tableName, row, column Text, value int64) (int64, error) {
	err, results := c.runCommand("AtomicIncrement", tableName, row, column, value)
	if err != nil {
		return int64(0), err
	}
	return results[0].Int(), nil
}

// Compact wraps HBase Compact method.
func (c *WrapConn) Compact(tableNameOrRegionName Bytes) error {
	err, _ := c.runCommand("Compact", tableNameOrRegionName)
	return err
}

// CreateTable wraps HBase CreateTable method.
func (c *WrapConn) CreateTable(tableName Text, columnFamilies []*ColumnDescriptor) error {
	err, _ := c.runCommand("CreateTable", tableName, columnFamilies)
	return err
}

// DeleteAll wraps HBase DeleteAll method.
func (c *WrapConn) DeleteAll(tableName, row, column Text, attributes map[string]Text) error {
	err, _ := c.runCommand("DeleteAll", tableName, row, column, attributes)
	return err
}

// DeleteAllRow wraps HBase DeleteAllRow method.
func (c *WrapConn) DeleteAllRow(tableName, row Text, attributes map[string]Text) error {
	err, _ := c.runCommand("DeleteAllRow", tableName, row, attributes)
	return err
}

// DeleteAllRowTs wraps HBase DeleteAllRowTs method.
func (c *WrapConn) DeleteAllRowTs(tableName, row Text, timestamp int64, attributes map[string]Text) error {
	err, _ := c.runCommand("DeleteAllRowTs", tableName, row, timestamp, attributes)
	return err
}

// DeleteAllTs wraps HBase DeleteAllTs method.
func (c *WrapConn) DeleteAllTs(tableName, row, column Text, timestamp int64, attributes map[string]Text) error {
	err, _ := c.runCommand("DeleteAllTs", tableName, row, column, timestamp, attributes)
	return err
}

// DeleteTable wraps HBase DeleteTable method.
func (c *WrapConn) DeleteTable(tableName Text) error {
	err, _ := c.runCommand("DeleteTable", tableName)
	return err
}

// DisableTable wraps HBase DisableTable method.
func (c *WrapConn) DisableTable(tableName Bytes) error {
	err, _ := c.runCommand("DisableTable", tableName)
	return err
}

// EnableTable wraps HBase EnableTable method.
func (c *WrapConn) EnableTable(tableName Bytes) error {
	err, _ := c.runCommand("EnableTable", tableName)
	return err
}

// Get wraps HBase Get method.
func (c *WrapConn) Get(tableName, row, column Text, attributes map[string]Text) ([]*TCell, error) {
	err, results := c.runCommand("Get", tableName, row, column, attributes)
	if err != nil {
		return nil, err
	}
	return results[0].Interface().([]*TCell), nil
}

// GetColumnDescriptors wraps HBase GetColumnDescriptors method.
func (c *WrapConn) GetColumnDescriptors(tableName Text) (map[string]*ColumnDescriptor, error) {
	err, results := c.runCommand("GetColumnDescriptors", tableName)
	if err != nil {
		return nil, err
	}
	return results[0].Interface().(map[string]*ColumnDescriptor), nil
}

// GetRegionInfo wraps HBase GetRegionInfo method.
func (c *WrapConn) GetRegionInfo(row Text) (*TRegionInfo, error) {
	err, results := c.runCommand("GetRegionInfo", row)
	if err != nil {
		return nil, err
	}
	return results[0].Interface().(*TRegionInfo), nil
}

// GetRow wraps HBase GetRow method.
func (c *WrapConn) GetRow(tableName, row Text, attributes map[string]Text) ([]*TRowResult_, error) {
	err, results := c.runCommand("GetRow", tableName, row, attributes)
	if err != nil {
		return nil, err
	}
	return results[0].Interface().([]*TRowResult_), nil
}

// GetRowOrBefore wraps HBase GetRowOrBefore method.
func (c *WrapConn) GetRowOrBefore(tableName, row Text, family Text) ([]*TCell, error) {
	err, results := c.runCommand("GetRowOrBefore", tableName, row, family)
	if err != nil {
		return nil, err
	}
	return results[0].Interface().([]*TCell), nil
}

// GetRowTs wraps HBase GetRowTs method.
func (c *WrapConn) GetRowTs(tableName, row Text, timestamp int64, attributes map[string]Text) ([]*TRowResult_, error) {
	err, results := c.runCommand("GetRowTs", tableName, row, timestamp, attributes)
	if err != nil {
		return nil, err
	}
	return results[0].Interface().([]*TRowResult_), nil
}

// GetRowWithColumns wraps HBase GetRowWithColumns method.
func (c *WrapConn) GetRowWithColumns(tableName, row Text, columns [][]byte, attributes map[string]Text) ([]*TRowResult_, error) {
	err, results := c.runCommand("GetRowWithColumns", tableName, row, columns, attributes)
	if err != nil {
		return nil, err
	}
	return results[0].Interface().([]*TRowResult_), nil
}

// GetRowWithColumnsTs wraps HBase GetRowWithColumnsTs method.
func (c *WrapConn) GetRowWithColumnsTs(tableName, row Text, columns [][]byte, timestamp int64, attributes map[string]Text) ([]*TRowResult_, error) {
	err, results := c.runCommand("GetRowWithColumnsTs", tableName, row, columns, timestamp, attributes)
	if err != nil {
		return nil, err
	}
	return results[0].Interface().([]*TRowResult_), nil
}

// GetRows wraps HBase GetRows method.
func (c *WrapConn) GetRows(tableName Text, rows [][]byte, attributes map[string]Text) ([]*TRowResult_, error) {
	err, results := c.runCommand("GetRows", tableName, rows, attributes)
	if err != nil {
		return nil, err
	}
	return results[0].Interface().([]*TRowResult_), nil
}

// GetRowsTs wraps HBase GetRowsTs method.
func (c *WrapConn) GetRowsTs(tableName Text, rows [][]byte, timestamp int64, attributes map[string]Text) ([]*TRowResult_, error) {
	err, results := c.runCommand("GetRowsTs", tableName, rows, timestamp, attributes)
	if err != nil {
		return nil, err
	}
	return results[0].Interface().([]*TRowResult_), nil
}

// GetRowsWithColumns wraps HBase GetRowsWithColumns method.
func (c *WrapConn) GetRowsWithColumns(tableName Text, rows [][]byte, columns [][]byte, attributes map[string]Text) ([]*TRowResult_, error) {
	err, results := c.runCommand("GetRowsWithColumns", tableName, rows, columns, attributes)
	if err != nil {
		return nil, err
	}
	return results[0].Interface().([]*TRowResult_), nil
}

// GetRowsWithColumnsTs wraps HBase GetRowsWithColumnsTs method.
func (c *WrapConn) GetRowsWithColumnsTs(tableName Text, rows [][]byte, columns [][]byte, timestamp int64, attributes map[string]Text) ([]*TRowResult_, error) {
	err, results := c.runCommand("GetRowsWithColumnsTs", tableName, rows, columns, timestamp, attributes)
	if err != nil {
		return nil, err
	}
	return results[0].Interface().([]*TRowResult_), nil
}

// GetTableNames wraps HBase GetTableNames method.
func (c *WrapConn) GetTableNames() ([][]byte, error) {
	err, results := c.runCommand("GetTableNames")
	if err != nil {
		return nil, err
	}
	return results[0].Interface().([][]byte), nil
}

// GetTableRegions wraps HBase GetTableRegions method.
func (c *WrapConn) GetTableRegions(tableName Text) ([]*TRegionInfo, error) {
	err, results := c.runCommand("GetTableRegions", tableName)
	if err != nil {
		return nil, err
	}
	return results[0].Interface().([]*TRegionInfo), nil
}

// GetVer wraps HBase GetVer method.
func (c *WrapConn) GetVer(tableName, row, column Text, numVersions int32, attributes map[string]Text) ([]*TCell, error) {
	err, results := c.runCommand("GetVer", tableName, row, column, numVersions, attributes)
	if err != nil {
		return nil, err
	}
	return results[0].Interface().([]*TCell), nil
}

// GetVerTs wraps HBase GetVerTs method.
func (c *WrapConn) GetVerTs(tableName, row, column Text, timestamp int64, numVersions int32, attributes map[string]Text) ([]*TCell, error) {
	err, results := c.runCommand("GetVerTs", tableName, row, column, timestamp, numVersions, attributes)
	if err != nil {
		return nil, err
	}
	return results[0].Interface().([]*TCell), nil
}

// Increment wraps HBase Increment method.
func (c *WrapConn) Increment(increment *TIncrement) error {
	err, _ := c.runCommand("Increment", increment)
	return err
}

// IncrementRows wraps HBase IncrementRows method.
func (c *WrapConn) IncrementRows(increments []*TIncrement) error {
	err, _ := c.runCommand("IncrementRows", increments)
	return err
}

// IsTableEnabled wraps HBase IsTableEnabled method.
func (c *WrapConn) IsTableEnabled(tableName Bytes) (bool, error) {
	err, results := c.runCommand("IsTableEnabled", tableName)
	if err != nil {
		return false, err
	}
	return results[0].Bool(), nil
}

// MajorCompact wraps HBase MajorCompact method.
func (c *WrapConn) MajorCompact(tableNameOrRegionName Bytes) error {
	err, _ := c.runCommand("MajorCompact", tableNameOrRegionName)
	return err
}

// MutateRow wraps HBase MutateRow method.
func (c *WrapConn) MutateRow(tableName, row Text, mutations []*Mutation, attributes map[string]Text) error {
	err, _ := c.runCommand("MutateRow", tableName, row, mutations, attributes)
	return err
}

// MutateRowTs wraps HBase MutateRowTs method.
func (c *WrapConn) MutateRowTs(tableName, row Text, mutations []*Mutation, timestamp int64, attributes map[string]Text) error {
	err, _ := c.runCommand("MutateRowTs", tableName, row, mutations, timestamp, attributes)
	return err
}

// MutateRows wraps HBase MutateRows method.
func (c *WrapConn) MutateRows(tableName Text, rowBatches []*BatchMutation, attributes map[string]Text) error {
	err, _ := c.runCommand("MutateRows", tableName, rowBatches, attributes)
	return err
}

// MutateRowsTs wraps HBase MutateRowsTs method.
func (c *WrapConn) MutateRowsTs(tableName Text, rowBatches []*BatchMutation, timestamp int64, attributes map[string]Text) error {
	err, _ := c.runCommand("MutateRowsTs", tableName, rowBatches, timestamp, attributes)
	return err
}

// ScannerClose wraps HBase ScannerClose method.
func (c *WrapConn) ScannerClose(id ScannerID) error {
	err, _ := c.runCommand("ScannerClose", id)
	return err
}

// ScannerGet wraps HBase ScannerGet method.
func (c *WrapConn) ScannerGet(id ScannerID) ([]*TRowResult_, error) {
	err, results := c.runCommand("ScannerGet", id)
	if err != nil {
		return nil, err
	}
	return results[0].Interface().([]*TRowResult_), nil
}

// ScannerGetList wraps HBase ScannerGetList method.
func (c *WrapConn) ScannerGetList(id ScannerID, nbRows int32) ([]*TRowResult_, error) {
	err, results := c.runCommand("ScannerGetList", id, nbRows)
	if err != nil {
		return nil, err
	}
	return results[0].Interface().([]*TRowResult_), nil
}

// ScannerOpen wraps HBase ScannerOpen method.
func (c *WrapConn) ScannerOpen(tableName, startRow Text, columns [][]byte, attributes map[string]Text) (ScannerID, error) {
	err, results := c.runCommand("ScannerOpen", tableName, startRow, columns, attributes)
	if err != nil {
		return ScannerID(0), err
	}
	return ScannerID(results[0].Int()), nil
}

// ScannerOpenTs wraps HBase ScannerOpenTs method.
func (c *WrapConn) ScannerOpenTs(tableName, startRow Text, columns [][]byte, timestamp int64, attributes map[string]Text) (ScannerID, error) {
	err, results := c.runCommand("ScannerOpenTs", tableName, startRow, columns, timestamp, attributes)
	if err != nil {
		return ScannerID(0), err
	}
	return ScannerID(results[0].Int()), nil
}

// ScannerOpenWithPrefix wraps HBase ScannerOpenWithPrefix method.
func (c *WrapConn) ScannerOpenWithPrefix(tableName, startAndPrefix Text, columns [][]byte, attributes map[string]Text) (ScannerID, error) {
	err, results := c.runCommand("ScannerOpenWithPrefix", tableName, startAndPrefix, columns, attributes)
	if err != nil {
		return ScannerID(0), err
	}
	return ScannerID(results[0].Int()), nil
}

// ScannerOpenWithScan wraps HBase ScannerOpenWithScan method.
func (c *WrapConn) ScannerOpenWithScan(tableName Text, scan *TScan, attributes map[string]Text) (ScannerID, error) {
	err, results := c.runCommand("ScannerOpenWithScan", tableName, scan, attributes)
	if err != nil {
		return ScannerID(0), err
	}
	return ScannerID(results[0].Int()), nil
}

// ScannerOpenWithStop wraps HBase ScannerOpenWithStop method.
func (c *WrapConn) ScannerOpenWithStop(tableName, startRow, stopRow Text, columns [][]byte, attributes map[string]Text) (ScannerID, error) {
	err, results := c.runCommand("ScannerOpenWithStop", tableName, startRow, stopRow, columns, attributes)
	if err != nil {
		return ScannerID(0), err
	}
	return ScannerID(results[0].Int()), nil
}

// ScannerOpenWithStopTs wraps HBase ScannerOpenWithStopTs method.
func (c *WrapConn) ScannerOpenWithStopTs(tableName, startRow, stopRow Text, columns [][]byte, timestamp int64, attributes map[string]Text) (ScannerID, error) {
	err, results := c.runCommand("ScannerOpenWithStopTs", tableName, startRow, stopRow, columns, timestamp, attributes)
	if err != nil {
		return ScannerID(0), err
	}
	return ScannerID(results[0].Int()), nil
}

// Appends values to one or more columns within a single row.
//
// @return values of columns after the append operation.
//
// Parameters:
//  - Append: The single append operation to apply
func (c *WrapConn) Append(append *TAppend) (r []*TCell, err error) {
	err, results := c.runCommand("Append", append)
	if err != nil {
		return nil, err
	}
	return results[0].Interface().([]*TCell), nil
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
func (c *WrapConn) CheckAndPut(tableName Text, row Text, column Text, value Text, mput *Mutation, attributes map[string]Text) (r bool, err error) {
	err, results := c.runCommand("CheckAndPut", tableName, row, column, value, mput, attributes)
	if err != nil {
		return false, err
	}
	return results[0].Bool(), nil
}
